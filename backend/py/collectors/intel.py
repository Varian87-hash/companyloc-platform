import random
import time

import requests

BASE = "https://intel.wd1.myworkdayjobs.com"
TENANT = "intel"
SITE = "External"

LIST_URL = f"{BASE}/wday/cxs/{TENANT}/{SITE}/jobs"

DETAIL_SLEEP_MIN = 0.35
DETAIL_SLEEP_MAX = 0.75

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Origin": BASE,
    "Referer": f"{BASE}/en-US/{SITE}",
}


def request_with_retry(
    method: str,
    url: str,
    *,
    json=None,
    headers=None,
    timeout=20,
    max_retries=5,
    base_backoff=1.0,
):
    h = dict(DEFAULT_HEADERS)
    if headers:
        h.update(headers)

    for attempt in range(max_retries + 1):
        try:
            resp = requests.request(method, url, json=json, headers=h, timeout=timeout)
            if resp.status_code in (429, 502, 503, 504):
                if attempt == max_retries:
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    wait = float(retry_after)
                else:
                    wait = base_backoff * (2**attempt)
                wait = min(60.0, wait) + random.uniform(0, 0.5)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except requests.RequestException:
            if attempt == max_retries:
                raise
            wait = base_backoff * (2**attempt) + random.uniform(0, 0.5)
            time.sleep(min(60.0, wait))

    raise RuntimeError("unreachable")


def fetch_intel_jobs_page(limit=20, offset=0, search_text=""):
    payload = {
        "appliedFacets": {},
        "limit": limit,
        "offset": offset,
        "searchText": search_text,
    }
    resp = request_with_retry("POST", LIST_URL, json=payload)
    return resp.json()


def fetch_all_intel_jobs(limit=20):
    first = fetch_intel_jobs_page(limit=limit, offset=0, search_text="")
    total = int(first.get("total", 0))
    postings = list(first.get("jobPostings", []))

    offset = len(postings)
    while offset < total:
        page = fetch_intel_jobs_page(limit=limit, offset=offset, search_text="")
        page_posts = page.get("jobPostings", [])
        postings.extend(page_posts)
        offset = len(postings)
        time.sleep(random.uniform(0.05, 0.15))

    return total, postings


def is_multi_location_text(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    return s.endswith("Locations") and s.split(" ")[0].isdigit()


def fetch_intel_job_detail_location_payload(external_path: str) -> dict:
    if not external_path or not external_path.startswith("/job/"):
        return {"country": None, "locations": []}

    detail_url = f"{BASE}/wday/cxs/{TENANT}/{SITE}{external_path}"

    try:
        resp = request_with_retry("GET", detail_url)
        data = resp.json()
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 403:
            return {"country": None, "locations": []}
        raise

    jpi = data.get("jobPostingInfo", {}) or {}

    country = jpi.get("country")
    if isinstance(country, dict):
        country = country.get("value") or country.get("descriptor")
    if isinstance(country, str):
        country = country.strip() or None
    else:
        country = None

    locs = []
    main_loc = jpi.get("location")
    if isinstance(main_loc, str) and main_loc.strip():
        locs.append(main_loc.strip())

    add_locs = jpi.get("additionalLocations")
    if isinstance(add_locs, list):
        for x in add_locs:
            if isinstance(x, str) and x.strip():
                locs.append(x.strip())

    seen = set()
    uniq = []
    for x in locs:
        if x not in seen:
            seen.add(x)
            uniq.append(x)

    return {"country": country, "locations": uniq}


def get_effective_locations_for_job(job_posting: dict) -> tuple[list[str], str | None]:
    loc_text = job_posting.get("locationsText")

    if isinstance(loc_text, str) and is_multi_location_text(loc_text):
        time.sleep(random.uniform(DETAIL_SLEEP_MIN, DETAIL_SLEEP_MAX))
        payload = fetch_intel_job_detail_location_payload(job_posting.get("externalPath"))
        locs = payload.get("locations") or []
        country = payload.get("country")
        if not locs:
            return [loc_text], None
        if not isinstance(country, str):
            country = None
        return locs, country

    if isinstance(loc_text, str) and loc_text.strip():
        return [loc_text.strip()], None

    return [], None
