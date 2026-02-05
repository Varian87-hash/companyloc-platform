from datetime import date, datetime, timezone
from typing import Optional, Tuple

from dotenv import load_dotenv

from backend.py.pipeline.common import (
    as_text,
    dedup_rows_by_confidence,
    get_company_id_by_name,
    normalize_country_iso2,
    stable_hash,
)

from backend.py.collectors.amazon import fetch_all_amazon_jobs
from backend.py.storage.neon import (
    refresh_mv_country_month_counts,
    upsert_job_location_facts,
)

def parse_location(loc: str, detail_country: Optional[str]) -> Tuple[Optional[str], Optional[str], str, float]:
    loc = as_text(loc)
    detail_country = detail_country or "UN"
    if not loc:
        return (None, None, detail_country, 0.0)

    parts = [p.strip() for p in loc.split(",") if p.strip()]
    if len(parts) >= 3 and len(parts[0]) == 2 and parts[0].isalpha():
        country = normalize_country_iso2(parts[0])
        region = parts[1]
        city = ", ".join(parts[2:])
        return (city or None, region or None, country, 0.95 if country != "UN" else 0.5)

    if len(parts) >= 2:
        city = parts[-1]
        maybe_country = normalize_country_iso2(parts[0])
        country = maybe_country if maybe_country != "UN" else detail_country
        return (city or None, None, country, 0.8 if country != "UN" else 0.3)

    return (loc, None, detail_country, 0.65 if detail_country != "UN" else 0.2)


def infer_country_from_location(loc: str) -> Optional[str]:
    s = as_text(loc)
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        return None
    c = normalize_country_iso2(parts[0])
    return None if c == "UN" else c


def _to_iso_utc_from_epoch(value: int | None) -> Optional[str]:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
    except Exception:
        return None


def main():
    load_dotenv()

    company_id = get_company_id_by_name("Amazon")
    captured_at = datetime.utcnow()
    snapshot_date = captured_at.date()
    snapshot_month = date.today().replace(day=1)

    total, postings = fetch_all_amazon_jobs(size=100)
    print("Amazon total:", total)
    print("Fetched postings:", len(postings))

    rows = []
    for p in postings:
        job_key = as_text(p.get("job_key"))
        title = as_text(p.get("title"))
        posted_at = _to_iso_utc_from_epoch(p.get("posted_on"))

        if not job_key:
            continue

        locs = p.get("locations") if isinstance(p.get("locations"), list) else []
        locs = [as_text(x) for x in locs]
        locs = [x for x in locs if x]
        if not locs:
            continue

        detail_country = infer_country_from_location(locs[0])
        detail_country = normalize_country_iso2(detail_country)

        job_hash = stable_hash(
            company_id,
            job_key,
            snapshot_date.isoformat(),
            title or "",
            locs,
        )

        for loc in locs:
            city_norm, region_norm, country_norm, conf = parse_location(loc, detail_country)
            rows.append(
                (
                    as_text(company_id),
                    as_text(job_key),
                    snapshot_month,
                    snapshot_date,
                    as_text(title),
                    None,
                    None,
                    as_text(loc),
                    as_text(city_norm),
                    as_text(region_norm),
                    normalize_country_iso2(country_norm),
                    float(conf or 0.0),
                    as_text(posted_at),
                    as_text(job_hash),
                    captured_at,
                )
            )
    rows = dedup_rows_by_confidence(rows)
    upsert_job_location_facts(rows)
    print("Inserted/updated rows:", len(rows))
    refresh_mv_country_month_counts()
    print("Refreshed trend MVs (count + avg variants if present)")


if __name__ == "__main__":
    main()
