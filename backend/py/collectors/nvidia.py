# backend/py/collectors/nvidia.py
import random
import time
import requests

BASE = "https://nvidia.wd5.myworkdayjobs.com"
TENANT = "nvidia"
SITE = "NVIDIAExternalCareerSite"

LIST_URL = f"{BASE}/wday/cxs/{TENANT}/{SITE}/jobs"

# 最稳策略：单线程 + 适度抖动
DETAIL_SLEEP_MIN = 0.45
DETAIL_SLEEP_MAX = 0.85

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Origin": BASE,
    "Referer": f"{BASE}/{SITE}/",
}


def request_with_retry(method: str, url: str, *, json=None, headers=None, timeout=20,
                       max_retries=5, base_backoff=1.0):
    """
    最稳重试策略：
    - 429/503/502/504: 指数退避 + jitter
    - 若存在 Retry-After 优先使用
    """
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
                    wait = base_backoff * (2 ** attempt)
                wait = min(60.0, wait) + random.uniform(0, 0.5)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except requests.RequestException:
            if attempt == max_retries:
                raise
            wait = base_backoff * (2 ** attempt) + random.uniform(0, 0.5)
            time.sleep(min(60.0, wait))

    raise RuntimeError("unreachable")

def fetch_nvidia_jobs_page(limit=20, offset=0, search_text=""):
    payload = {
        "appliedFacets": {},
        "limit": limit,
        "offset": offset,
        "searchText": search_text,
    }
    resp = request_with_retry("POST", LIST_URL, json=payload)
    return resp.json()

def _fetch_all_nvidia_jobs_for_search(limit=20, search_text=""):
    first = fetch_nvidia_jobs_page(limit=limit, offset=0, search_text=search_text)
    total = int(first.get("total", 0))
    postings = list(first.get("jobPostings", []))

    offset = len(postings)
    while offset < total:
        page = fetch_nvidia_jobs_page(limit=limit, offset=offset, search_text=search_text)
        page_posts = page.get("jobPostings", [])
        postings.extend(page_posts)
        offset = len(postings)
        time.sleep(random.uniform(0.05, 0.15))

    return total, postings


def fetch_all_nvidia_jobs(limit=20, extra_search_texts=None):
    """
    全量分页：直到累计 jobPostings == total
    返回：list[dict]（每个 dict 含 externalPath/title/locationsText/postedOn 等）
    """
    total, postings = _fetch_all_nvidia_jobs_for_search(limit=limit, search_text="")

    # Workday broad query can cap at 2000. Merge targeted searches by externalPath.
    if extra_search_texts:
        seen = set()
        merged = []
        for p in postings:
            k = p.get("externalPath")
            if k:
                seen.add(k)
            merged.append(p)

        for q in extra_search_texts:
            if not isinstance(q, str) or not q.strip():
                continue
            _, sub = _fetch_all_nvidia_jobs_for_search(limit=limit, search_text=q.strip())
            for p in sub:
                k = p.get("externalPath")
                if k and k in seen:
                    continue
                if k:
                    seen.add(k)
                merged.append(p)

        postings = merged

    return max(total, len(postings)), postings

def fetch_nvidia_job_detail_locations(external_path: str) -> list[str]:
    """
    NVIDIA 已验证结构：
      jobPostingInfo.location (str)
      jobPostingInfo.additionalLocations (list[str])  # 多地点
      jobPostingInfo.country (存在，可用于后续补齐国家)
    """
    if not external_path or not external_path.startswith("/job/"):
        return []

    detail_url = f"{BASE}/wday/cxs/{TENANT}/{SITE}{external_path}"
    resp = request_with_retry("GET", detail_url)
    data = resp.json()
    jpi = data.get("jobPostingInfo", {}) or {}

    out = []
    main_loc = jpi.get("location")
    if isinstance(main_loc, str) and main_loc.strip():
        out.append(main_loc.strip())

    add_locs = jpi.get("additionalLocations")
    if isinstance(add_locs, list):
        for x in add_locs:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())

    # 去重保序
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

def is_multi_location_text(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    # e.g. "2 Locations"
    return s.endswith("Locations") and s.split(" ")[0].isdigit()

def get_effective_locations_for_job(job_posting: dict) -> tuple[list[str], str | None]:
    loc_text = job_posting.get("locationsText")

    if isinstance(loc_text, str) and is_multi_location_text(loc_text):
        time.sleep(random.uniform(DETAIL_SLEEP_MIN, DETAIL_SLEEP_MAX))
        payload = fetch_nvidia_job_detail_location_payload(job_posting.get("externalPath"))
        locs = payload.get("locations") or []
        country = payload.get("country")

        # 详情被403拦了：降级为单条 raw（避免整个任务失败）
        if not locs:
            return [loc_text], None

        if not isinstance(country, str):
            country = None
        return locs, country

    # Single-location postings: keep list page location text.
    if isinstance(loc_text, str) and loc_text.strip():
        return [loc_text.strip()], None

    return [], None


def fetch_nvidia_job_detail_location_payload(external_path: str) -> dict:
    if not external_path or not external_path.startswith("/job/"):
        return {"country": None, "locations": []}

    detail_url = f"{BASE}/wday/cxs/{TENANT}/{SITE}{external_path}"

    try:
        resp = request_with_retry("GET", detail_url)
        data = resp.json()
    except requests.HTTPError as e:
        # 403: 直接降级（不让 pipeline 崩）
        if e.response is not None and e.response.status_code == 403:
            return {"country": None, "locations": []}
        raise

    jpi = data.get("jobPostingInfo", {}) or {}

    country = jpi.get("country")
    # 强制把 country 变成 str/None，避免 dict 下游炸
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

    # 去重保序
    seen = set()
    uniq = []
    for x in locs:
        if x not in seen:
            seen.add(x)
            uniq.append(x)

    return {"country": country, "locations": uniq}
