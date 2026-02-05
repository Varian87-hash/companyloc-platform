import random
import time
from typing import Any

import requests

SEARCH_URL = "https://jobs.apple.com/api/v1/search"

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Origin": "https://jobs.apple.com",
    "Referer": "https://jobs.apple.com/en-us/search",
}


def request_with_retry(
    method: str,
    url: str,
    *,
    params: dict | None = None,
    json: dict | None = None,
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
            resp = requests.request(
                method,
                url,
                params=params,
                json=json,
                headers=h,
                timeout=timeout,
            )
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt == max_retries:
                    resp.raise_for_status()
                wait = base_backoff * (2**attempt) + random.uniform(0, 0.4)
                time.sleep(min(30.0, wait))
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException:
            if attempt == max_retries:
                raise
            wait = base_backoff * (2**attempt) + random.uniform(0, 0.4)
            time.sleep(min(30.0, wait))

    raise RuntimeError("unreachable")


def _normalize_location(loc: dict[str, Any]) -> str | None:
    if not isinstance(loc, dict):
        return None
    city = loc.get("city")
    state = loc.get("stateProvince")
    country = loc.get("countryName")
    name = loc.get("name")

    parts = []
    for x in (city, state, country):
        if isinstance(x, str) and x.strip():
            parts.append(x.strip())

    if parts:
        return ", ".join(parts)
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


def _normalize_job(job: dict[str, Any]) -> dict:
    req_id = job.get("reqId") or job.get("id") or job.get("jobPositionId")
    title = job.get("postingTitle")
    posting_date = job.get("postDateInGMT") or job.get("postingDate")

    locations_raw = job.get("locations")
    locations: list[str] = []
    if isinstance(locations_raw, list):
        for loc in locations_raw:
            if isinstance(loc, dict):
                s = _normalize_location(loc)
                if s:
                    locations.append(s)
            elif isinstance(loc, str) and loc.strip():
                locations.append(loc.strip())

    seen = set()
    uniq = []
    for x in locations:
        if x not in seen:
            seen.add(x)
            uniq.append(x)

    return {
        "job_key": str(req_id).strip() if req_id is not None else None,
        "title": title.strip() if isinstance(title, str) else (str(title) if title is not None else None),
        "locations": uniq,
        "posted_on": posting_date,
    }


def fetch_apple_jobs_page(page: int = 1) -> tuple[int, list[dict]]:
    body = {
        "query": "",
        "filters": {},
        "page": page,
        "locale": "en-us",
        "sort": "newest",
        "format": {
            "longDate": "MMMM D, YYYY",
            "mediumDate": "MMM D, YYYY",
        },
    }
    resp = request_with_retry("POST", SEARCH_URL, json=body)
    payload = resp.json()
    res = payload.get("res", {}) if isinstance(payload, dict) else {}
    total = int(res.get("totalRecords", 0) or 0)
    items = res.get("searchResults", [])
    if not isinstance(items, list):
        items = []
    return total, items


def fetch_all_apple_jobs(max_pages: int = 500) -> tuple[int, list[dict]]:
    all_jobs: list[dict] = []
    total_hint = 0

    for page in range(1, max_pages + 1):
        total, items = fetch_apple_jobs_page(page=page)
        total_hint = max(total_hint, total)

        if not items:
            break

        for item in items:
            if not isinstance(item, dict):
                continue
            all_jobs.append(_normalize_job(item))

        if total_hint and len(all_jobs) >= total_hint:
            break

        time.sleep(random.uniform(0.08, 0.2))

    seen = set()
    uniq = []
    for j in all_jobs:
        k = j.get("job_key")
        if not k or k in seen:
            continue
        seen.add(k)
        uniq.append(j)

    return total_hint or len(uniq), uniq
