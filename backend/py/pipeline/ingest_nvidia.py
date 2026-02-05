# backend/py/pipeline/ingest_nvidia.py
from datetime import datetime, date
from typing import Optional, Tuple

from dotenv import load_dotenv

from backend.py.pipeline.common import (
    as_text,
    dedup_rows_by_confidence,
    get_company_id_by_name,
    normalize_country_iso2,
    stable_hash,
)

from backend.py.collectors.nvidia import (
    fetch_all_nvidia_jobs,
    get_effective_locations_for_job,  # MUST return (locs: list[str], detail_country: str|None)
)
from backend.py.storage.neon import (
    refresh_mv_country_month_counts,
    upsert_job_location_facts,
)

# ---------------------------
# Location normalization
# ---------------------------
def parse_location(loc: str, detail_country: Optional[str]) -> Tuple[Optional[str], Optional[str], str, float]:
    """
    Normalize to (city_norm, region_norm, country_norm, confidence)

    Rules:
      - "US, CA, Santa Clara" -> country=US, region=CA, city="Santa Clara"
      - "Germany, Munich" / "Israel, Yokneam" -> use detail_country as ISO-2 fallback
      - "Remote" / unstructured -> use detail_country if present else UN
    """
    loc = as_text(loc)
    # detail_country is expected to be normalized by caller.
    detail_country = detail_country or "UN"

    if not loc:
        return (None, None, detail_country, 0.0)

    parts = [p.strip() for p in loc.split(",") if p.strip()]

    # Typical Workday list format for US: "US, CA, Santa Clara"
    if len(parts) >= 3 and len(parts[0]) == 2 and parts[0].isupper():
        country = parts[0]
        region = parts[1]
        city = ", ".join(parts[2:])
        return (city or None, region or None, country, 0.95)

    # Common non-US list: "Germany, Munich" / "Israel, Yokneam"
    if len(parts) >= 2:
        city = parts[-1]
        first = parts[0]
        inferred_country = normalize_country_iso2(first)
        country = inferred_country if inferred_country != "UN" else detail_country
        conf = 0.9 if inferred_country != "UN" else (0.85 if detail_country != "UN" else 0.2)
        return (city or None, None, country, conf)

    # Fallback single token: "Remote" etc.
    return (loc or None, None, detail_country, 0.7 if detail_country != "UN" else 0.1)


# ---------------------------
# Main
# ---------------------------
def main():
    load_dotenv()

    company_id = get_company_id_by_name("NVIDIA")
    captured_at = datetime.utcnow()
    snapshot_date = captured_at.date()
    snapshot_month = date.today().replace(day=1)

    total, postings = fetch_all_nvidia_jobs(limit=20, extra_search_texts=["canada"])
    print("Workday total:", total)
    print("Fetched postings:", len(postings))

    # Keep posted_at optional; normalize to text if you enable it.
    posted_at = None

    # Build row tuples to match storage.neon.upsert_job_location_facts SQL column order:
    # (company_id, job_key, snapshot_month, snapshot_date, title,
    #  city_raw, country_raw, location_raw,
    #  city_norm, region_norm, country_norm,
    #  location_confidence, posted_at, job_hash, captured_at)
    rows = []

    for p in postings:
        job_key = as_text(p.get("externalPath"))
        title = as_text(p.get("title"))

        # Optional:
        # posted_at = as_text(p.get("postedOn"))

        if not job_key:
            continue

        locs, detail_country = get_effective_locations_for_job(p)

        # Defensive: ensure types
        if not isinstance(locs, list):
            locs = []
        normalized_locs = []
        for x in locs:
            sx = as_text(x)
            if sx:
                normalized_locs.append(sx)
        locs = normalized_locs
        detail_country = normalize_country_iso2(detail_country)

        if not locs:
            continue

        job_hash = stable_hash(
            company_id,
            job_key,
            snapshot_date.isoformat(),
            title or "",
            locs,
        )

        for loc in locs:
            city_norm, region_norm, country_norm, conf = parse_location(loc, detail_country)

            # Ensure everything passed to psycopg2 is scalar (no dict/list)
            rows.append((
                as_text(company_id),               # uuid string
                as_text(job_key),
                snapshot_month,                    # date
                snapshot_date,                     # date
                as_text(title),
                None,                              # city_raw
                None,                              # country_raw
                as_text(loc),                      # location_raw
                as_text(city_norm),
                as_text(region_norm),
                normalize_country_iso2(country_norm),
                float(conf or 0.0),
                as_text(posted_at),
                as_text(job_hash),
                captured_at,                       # datetime
            ))

    # --- De-dup within the same command to satisfy Postgres ON CONFLICT restriction ---
    # Conflict key in DB: (company_id, job_key, snapshot_date, country_norm, COALESCE(city_norm,''))
    rows = dedup_rows_by_confidence(rows)
    # --- end de-dup ---

    upsert_job_location_facts(rows)
    print("Inserted/updated rows:", len(rows))
    refresh_mv_country_month_counts()
    print("Refreshed trend MVs (count + avg variants if present)")


if __name__ == "__main__":
    main()
