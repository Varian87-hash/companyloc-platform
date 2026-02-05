import random
import time
from typing import Any

import requests

SEARCH_URL = "https://www.amazon.jobs/api/jobs/search"
SEARCH_KEY = "PbxxNwIlTi4FP5oijKdtk3IrBF5CLd4R4oPHsKNh"

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Origin": "https://www.amazon.jobs",
    "Referer": "https://www.amazon.jobs/content/en/locations",
    "x-api-key": SEARCH_KEY,
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


def _first(v: Any) -> Any:
    if isinstance(v, list):
        return v[0] if v else None
    return v


def _normalize_job(hit: dict[str, Any]) -> dict:
    f = hit.get("fields", {}) if isinstance(hit, dict) else {}
    job_key = _first(f.get("icimsJobId"))
    title = _first(f.get("title"))
    location = _first(f.get("location")) or _first(f.get("normalizedLocation"))
    posted_raw = _first(f.get("updatedDate")) or _first(f.get("createdDate"))

    posted_on = None
    if isinstance(posted_raw, (int, float)):
        posted_on = int(posted_raw)
    elif isinstance(posted_raw, str) and posted_raw.strip().isdigit():
        posted_on = int(posted_raw.strip())

    locations: list[str] = []
    if isinstance(location, str) and location.strip():
        locations.append(location.strip())

    return {
        "job_key": str(job_key).strip() if job_key is not None else None,
        "title": title.strip() if isinstance(title, str) else (str(title) if title is not None else None),
        "locations": locations,
        "posted_on": posted_on,
    }


def fetch_amazon_jobs_page(size: int = 100, start: int = 0) -> tuple[int, int, list[dict]]:
    body = {
        "locale": "en-US",
        "start": start,
        "size": size,
    }
    try:
        resp = request_with_retry("POST", SEARCH_URL, json=body)
    except requests.HTTPError as e:
        text = ""
        if e.response is not None and isinstance(e.response.text, str):
            text = e.response.text
        if e.response is not None and e.response.status_code == 400 and "item number 10,000" in text:
            return 10000, start, []
        raise
    payload = resp.json()
    total = int(payload.get("found", 0) or 0)
    current_start = int(payload.get("start", start) or 0)
    hits = payload.get("searchHits", [])
    if not isinstance(hits, list):
        hits = []
    return total, current_start, hits


def fetch_all_amazon_jobs(size: int = 100, max_pages: int = 500) -> tuple[int, list[dict]]:
    all_jobs: list[dict] = []
    total_hint = 0
    start = 0

    for _ in range(max_pages):
        if start >= 10000:
            break
        total, current_start, hits = fetch_amazon_jobs_page(size=size, start=start)
        total_hint = max(total_hint, total)

        if not hits:
            break

        for hit in hits:
            if not isinstance(hit, dict):
                continue
            all_jobs.append(_normalize_job(hit))

        start = current_start + len(hits)
        if total_hint and start >= total_hint:
            break
        if len(hits) < size:
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
