import random
import re
import time

import requests

CAREERS_URL = "https://apply.careers.microsoft.com/careers?hl=en"
SEARCH_URL = "https://apply.careers.microsoft.com/api/pcsx/search"

DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
}


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int = 30,
    max_retries: int = 4,
    base_backoff: float = 1.0,
):
    h = dict(DEFAULT_HEADERS)
    if headers:
        h.update(headers)

    for attempt in range(max_retries + 1):
        try:
            resp = session.request(method, url, params=params, headers=h, timeout=timeout)
            if resp.status_code in (400, 429, 500, 502, 503, 504):
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


def _bootstrap_session() -> tuple[requests.Session, str, str]:
    session = requests.Session()
    resp = request_with_retry(
        session,
        "GET",
        CAREERS_URL,
        headers={"Accept-Encoding": "identity"},
    )
    text = resp.text
    m_csrf = re.search(r'<meta name="_csrf" content="([^"]+)"', text)
    if not m_csrf:
        raise RuntimeError("failed to parse _csrf from microsoft careers page")
    csrf = m_csrf.group(1)

    m_gid = re.search(r'window\._EF_GROUP_ID\s*=\s*"([^"]+)"', text)
    group_id = m_gid.group(1) if m_gid else "microsoft.com"
    return session, csrf, group_id


def _normalize_locations(raw: list[str] | None, fallback: list[str] | None) -> list[str]:
    blocked = {"multiple locations", "various locations", "multiple", "various"}
    locs: list[str] = []
    for source in (raw, fallback):
        if isinstance(source, list):
            for arr in source:
                if isinstance(arr, str):
                    s = arr.strip()
                    if s and s.lower() not in blocked:
                        locs.append(s)
        elif isinstance(source, str):
            s = source.strip()
            if s and s.lower() not in blocked:
                locs.append(s)
    seen = set()
    uniq = []
    for x in locs:
        key = x.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(x)
    return uniq


def _normalize_job(job: dict) -> dict:
    job_key = job.get("id") or job.get("displayJobId")
    title = job.get("name")
    locations = _normalize_locations(job.get("standardizedLocations"), job.get("locations"))
    posted_on = job.get("postedTs")
    return {
        "job_key": str(job_key).strip() if job_key is not None else None,
        "title": title.strip() if isinstance(title, str) else (str(title) if title is not None else None),
        "locations": locations,
        "posted_on": posted_on,
    }


def fetch_microsoft_jobs_page(start: int = 0, query: str = "", location: str = "") -> tuple[int | None, list[dict]]:
    session, csrf, group_id = _bootstrap_session()
    params = {
        "domain": group_id,
        "query": query or "",
        "location": location or "",
        "start": max(0, int(start)),
        "hl": "en",
    }
    headers = {
        "X-CSRF-Token": csrf,
        "X-EF-GROUP-ID": group_id,
        "X-EF-USER": "",
        "X-User-Timezone": "America/Los_Angeles",
        "Referer": CAREERS_URL,
    }
    resp = request_with_retry(session, "GET", SEARCH_URL, params=params, headers=headers)
    payload = resp.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None, []
    total = data.get("count")
    if isinstance(total, str) and total.isdigit():
        total = int(total)
    if not isinstance(total, int):
        total = None
    positions = data.get("positions")
    if not isinstance(positions, list):
        positions = []
    jobs = [_normalize_job(x) for x in positions if isinstance(x, dict)]
    return total, jobs


def fetch_all_microsoft_jobs(page_size: int = 10, max_pages: int = 500) -> tuple[int, list[dict]]:
    del page_size  # server side fixed to 10
    session, csrf, group_id = _bootstrap_session()

    all_jobs: list[dict] = []
    total_hint: int | None = None
    seen: set[str] = set()
    start = 0
    no_new_streak = 0

    for _ in range(max_pages):
        params = {
            "domain": group_id,
            "query": "",
            "location": "",
            "start": start,
            "hl": "en",
        }
        headers = {
            "X-CSRF-Token": csrf,
            "X-EF-GROUP-ID": group_id,
            "X-EF-USER": "",
            "X-User-Timezone": "America/Los_Angeles",
            "Referer": CAREERS_URL,
        }
        resp = request_with_retry(session, "GET", SEARCH_URL, params=params, headers=headers)
        payload = resp.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            break

        total = data.get("count")
        if isinstance(total, str) and total.isdigit():
            total = int(total)
        if isinstance(total, int):
            total_hint = total

        positions = data.get("positions")
        if not isinstance(positions, list) or not positions:
            break

        added = 0
        for raw in positions:
            if not isinstance(raw, dict):
                continue
            item = _normalize_job(raw)
            k = item.get("job_key")
            if not k or k in seen:
                continue
            seen.add(k)
            all_jobs.append(item)
            added += 1

        no_new_streak = no_new_streak + 1 if added == 0 else 0
        if no_new_streak >= 3:
            break
        if total_hint is not None and len(seen) >= total_hint:
            break

        start += len(positions)
        time.sleep(random.uniform(0.06, 0.14))

    return total_hint or len(all_jobs), all_jobs
