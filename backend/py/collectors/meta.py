import random
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests

SITEMAP_URL = "https://www.metacareers.com/jobs/sitemap.xml"

DEFAULT_HEADERS = {
    "Accept": "*/*",
    # Meta blocks some desktop UA strings on sitemap/job pages.
    "User-Agent": "Mozilla/5.0",
}


def request_with_retry(
    method: str,
    url: str,
    *,
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
            resp = requests.request(method, url, headers=h, timeout=timeout)
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


def _parse_sitemap_urls(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for loc in root.findall("sm:url/sm:loc", ns):
        if loc.text and "/profile/job_details/" in loc.text:
            urls.append(loc.text.strip())
    return urls


def fetch_meta_job_detail_urls() -> list[str]:
    resp = request_with_retry("GET", SITEMAP_URL)
    return _parse_sitemap_urls(resp.text)


def _extract_ld_json(html: str) -> dict[str, Any] | None:
    marker = '<script type="application/ld+json"'
    start = html.find(marker)
    if start == -1:
        return None
    open_end = html.find(">", start)
    if open_end == -1:
        return None
    close = html.find("</script>", open_end)
    if close == -1:
        return None

    raw = html[open_end + 1 : close]
    try:
        # Meta renders JSON-LD with escaped forward slashes.
        unescaped = raw.encode("utf-8").decode("unicode_escape")
        data = requests.models.complexjson.loads(unescaped)
        if isinstance(data, dict):
            return data
    except Exception:
        return None
    return None


def _extract_locations(ldj: dict[str, Any]) -> list[str]:
    out: list[str] = []
    raw = ldj.get("jobLocation")
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if isinstance(name, str) and name.strip():
                out.append(name.strip())
    elif isinstance(raw, dict):
        name = raw.get("name")
        if isinstance(name, str) and name.strip():
            out.append(name.strip())

    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def fetch_meta_job_detail(job_url: str) -> dict | None:
    resp = request_with_retry("GET", job_url)
    ldj = _extract_ld_json(resp.text)
    if not ldj:
        return None

    title = ldj.get("title")
    posted_on = ldj.get("datePosted")
    locations = _extract_locations(ldj)
    job_key = job_url.rstrip("/").split("/")[-1]

    return {
        "job_key": str(job_key).strip() if job_key else None,
        "title": title.strip() if isinstance(title, str) else (str(title) if title is not None else None),
        "locations": locations,
        "posted_on": posted_on if isinstance(posted_on, str) else None,
    }


def fetch_all_meta_jobs(max_jobs: int | None = None) -> tuple[int, list[dict]]:
    urls = fetch_meta_job_detail_urls()
    if max_jobs is not None:
        urls = urls[: max(0, int(max_jobs))]

    jobs: list[dict] = []
    for idx, u in enumerate(urls):
        item = fetch_meta_job_detail(u)
        if isinstance(item, dict):
            jobs.append(item)
        if idx < len(urls) - 1:
            time.sleep(random.uniform(0.04, 0.12))

    seen = set()
    uniq = []
    for j in jobs:
        k = j.get("job_key")
        if not k or k in seen:
            continue
        seen.add(k)
        uniq.append(j)

    return len(urls), uniq
