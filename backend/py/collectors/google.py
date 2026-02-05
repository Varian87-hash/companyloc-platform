import html
import random
import re
import time

import requests

RESULTS_URL = "https://www.google.com/about/careers/applications/jobs/results"

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "identity",
    "User-Agent": "Mozilla/5.0",
}


def request_with_retry(
    method: str,
    url: str,
    *,
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


def _results_url(page: int) -> str:
    return RESULTS_URL if page <= 1 else f"{RESULTS_URL}?page={page}"


def _clean_text(raw: str | None) -> str:
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", " ", raw)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_total_jobs(html_text: str) -> int | None:
    m = re.search(r'<span class="SWhIm">([0-9,]+)</span>\s*jobs matched', html_text)
    if not m:
        m = re.search(r"([0-9][0-9,]*)\s+jobs matched", html_text)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", "").strip())
    except Exception:
        return None


def _extract_job_cards(html_text: str) -> list[dict]:
    out: list[dict] = []
    parts = re.split(r'<li class="lLd3Je"[^>]*>', html_text)
    if len(parts) <= 1:
        return out

    for part in parts[1:]:
        card = part.split("</li>", 1)[0]

        id_match = re.search(r'jsdata="Aiqs8c;(\d+);', card)
        href_match = re.search(r'href="(jobs/results/[^"]+)"', card)
        title_match = re.search(r'<h3 class="QJPWVe">(.*?)</h3>', card, flags=re.S)

        if not id_match and not href_match:
            continue

        job_key = id_match.group(1) if id_match else _clean_text(href_match.group(1))
        title = _clean_text(title_match.group(1) if title_match else "")

        location_bits = re.findall(r'<span class="r0wTof[^"]*">(.*?)</span>', card, flags=re.S)
        location_joined = "; ".join(_clean_text(x).lstrip(";").strip() for x in location_bits if _clean_text(x))
        locations = [x.strip() for x in location_joined.split(";") if x.strip()]

        seen = set()
        uniq_locations = []
        for loc in locations:
            if loc in seen:
                continue
            seen.add(loc)
            uniq_locations.append(loc)

        out.append(
            {
                "job_key": str(job_key).strip(),
                "title": title,
                "locations": uniq_locations,
                "posted_on": None,
            }
        )

    return out


def _fetch_google_results_page(page: int) -> tuple[list[dict], int | None]:
    resp = request_with_retry("GET", _results_url(page))
    html_text = resp.text
    return _extract_job_cards(html_text), _extract_total_jobs(html_text)


def fetch_google_jobs_page(page: int = 1, limit: int = 50) -> tuple[list[dict], int | None]:
    del limit  # kept for compatibility with previous function signature
    return _fetch_google_results_page(page)


def fetch_all_google_jobs(limit: int = 50, max_pages: int = 250) -> tuple[int, list[dict]]:
    del limit  # Google page size is controlled by site, not by API args
    all_jobs: list[dict] = []
    total_hint: int | None = None
    seen_keys: set[str] = set()
    no_new_streak = 0

    for page in range(1, max_pages + 1):
        jobs, total = _fetch_google_results_page(page)
        if total is not None:
            total_hint = total

        added = 0
        for j in jobs:
            key = str(j.get("job_key") or "").strip()
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            all_jobs.append(j)
            added += 1

        no_new_streak = no_new_streak + 1 if added == 0 else 0
        if total_hint is not None and len(seen_keys) >= total_hint:
            break
        if no_new_streak >= 3:
            break

        time.sleep(random.uniform(0.04, 0.12))

    return total_hint or len(all_jobs), all_jobs
