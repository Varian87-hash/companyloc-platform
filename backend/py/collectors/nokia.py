import random
import time
from typing import Any

import requests

BASE = "https://fa-evmr-saasfaprod1.fa.ocs.oraclecloud.com:443/hcmRestApi/resources/latest"
JOBS_URL = f"{BASE}/recruitingCEJobRequisitions"
SITE_NUMBER = "CX_1"

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
}

EXPAND = (
    "requisitionList.workLocation,"
    "requisitionList.otherWorkLocations,"
    "requisitionList.secondaryLocations,"
    "requisitionList.requisitionFlexFields"
)


def request_with_retry(
    method: str,
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int = 25,
    max_retries: int = 4,
    base_backoff: float = 1.0,
):
    h = dict(DEFAULT_HEADERS)
    if headers:
        h.update(headers)

    for attempt in range(max_retries + 1):
        try:
            resp = requests.request(method, url, params=params, headers=h, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt == max_retries:
                    resp.raise_for_status()
                wait = base_backoff * (2**attempt) + random.uniform(0, 0.5)
                time.sleep(min(30.0, wait))
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException:
            if attempt == max_retries:
                raise
            wait = base_backoff * (2**attempt) + random.uniform(0, 0.5)
            time.sleep(min(30.0, wait))

    raise RuntimeError("unreachable")


def _finder(limit: int, offset: int) -> str:
    return f"findReqs;siteNumber={SITE_NUMBER},limit={limit},offset={offset}"


def fetch_nokia_jobs_page(limit: int = 24, offset: int = 0) -> tuple[int, list[dict]]:
    params = {
        "onlyData": "true",
        "expand": EXPAND,
        "finder": _finder(limit=limit, offset=offset),
    }
    resp = request_with_retry("GET", JOBS_URL, params=params)
    data = resp.json()
    items = data.get("items", [])
    if not items:
        return 0, []
    node = items[0] if isinstance(items[0], dict) else {}
    total = int(node.get("TotalJobsCount", 0) or 0)
    reqs = node.get("requisitionList", [])
    if not isinstance(reqs, list):
        reqs = []
    return total, reqs


def _work_location_to_text(loc: dict[str, Any]) -> str | None:
    city = loc.get("TownOrCity")
    region = loc.get("Region1") or loc.get("Region2") or loc.get("Region3")
    country = loc.get("Country")
    parts = []
    for x in (city, region, country):
        if isinstance(x, str) and x.strip():
            parts.append(x.strip())
    if parts:
        return ", ".join(parts)
    name = loc.get("LocationName")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


def _extract_locations(job: dict[str, Any]) -> list[str]:
    out: list[str] = []

    primary = job.get("PrimaryLocation")
    if isinstance(primary, str) and primary.strip():
        out.append(primary.strip())

    for key in ("workLocation", "otherWorkLocations"):
        vals = job.get(key)
        if isinstance(vals, list):
            for loc in vals:
                if isinstance(loc, dict):
                    s = _work_location_to_text(loc)
                    if s:
                        out.append(s)

    sec = job.get("secondaryLocations")
    if isinstance(sec, list):
        for x in sec:
            if not isinstance(x, dict):
                continue
            name = x.get("Name")
            country = x.get("CountryCode")
            if isinstance(name, str) and name.strip():
                if isinstance(country, str) and country.strip():
                    out.append(f"{name.strip()}, {country.strip()}")
                else:
                    out.append(name.strip())

    # unique keep order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _normalize_job(j: dict[str, Any]) -> dict:
    job_id = j.get("Id")
    title = j.get("Title")
    posted = j.get("PostedDate")
    locations = _extract_locations(j)
    return {
        "job_key": str(job_id).strip() if job_id is not None else None,
        "title": title.strip() if isinstance(title, str) else (str(title) if title is not None else None),
        "locations": locations,
        "posted_on": posted,
    }


def fetch_all_nokia_jobs(limit: int = 24, max_pages: int = 300) -> tuple[int, list[dict]]:
    all_jobs: list[dict] = []
    total_hint = 0
    offset = 0
    page_count = 0

    while page_count < max_pages:
        total, reqs = fetch_nokia_jobs_page(limit=limit, offset=offset)
        total_hint = max(total_hint, total)
        if not reqs:
            break

        for j in reqs:
            if not isinstance(j, dict):
                continue
            all_jobs.append(_normalize_job(j))

        if len(reqs) < limit:
            break

        offset += len(reqs)
        page_count += 1
        if total_hint and len(all_jobs) >= total_hint:
            break
        time.sleep(random.uniform(0.08, 0.2))

    # de-dup by job_key
    seen = set()
    uniq = []
    for j in all_jobs:
        k = j.get("job_key")
        if not k or k in seen:
            continue
        seen.add(k)
        uniq.append(j)

    return total_hint or len(uniq), uniq
