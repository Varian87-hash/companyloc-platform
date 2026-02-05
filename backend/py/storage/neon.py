import json
import os
import sys
import psycopg2
from psycopg2.extras import execute_values

def get_conn():
    url = os.environ.get("NEON_DATABASE_URL")
    if not url:
        raise RuntimeError("NEON_DATABASE_URL is not set")
    return psycopg2.connect(url)

def upsert_job_location_facts(rows):
    if not rows:
        return

    columns = [
        "company_id", "job_key", "snapshot_month", "snapshot_date", "title",
        "city_raw", "country_raw", "location_raw",
        "city_norm", "region_norm", "country_norm",
        "location_confidence", "posted_at", "job_hash", "captured_at",
    ]

    offenders = []

    # Defensive sanitize to avoid psycopg2 "can't adapt type 'dict'" failures.
    safe_rows = []
    for row_idx, row in enumerate(rows):
        safe_row = []
        for col_idx, v in enumerate(row):
            if isinstance(v, dict) or isinstance(v, list):
                col = columns[col_idx] if col_idx < len(columns) else f"col_{col_idx}"
                offenders.append((row_idx, col_idx, col, type(v).__name__, repr(v)[:200]))
                safe_row.append(json.dumps(v, ensure_ascii=False))
            else:
                safe_row.append(v)
        safe_rows.append(tuple(safe_row))

    if offenders:
        print("[upsert_job_location_facts] non-scalar params detected (sanitized):", file=sys.stderr)
        for row_idx, col_idx, col, type_name, preview in offenders[:10]:
            print(
                f"  row={row_idx} col={col_idx} ({col}) type={type_name} value={preview}",
                file=sys.stderr,
            )
        if len(offenders) > 10:
            print(f"  ... and {len(offenders) - 10} more", file=sys.stderr)

    sql = """
    INSERT INTO job_location_facts (
      company_id, job_key, snapshot_month, snapshot_date, title,
      city_raw, country_raw, location_raw,
      city_norm, region_norm, country_norm,
      location_confidence, posted_at, job_hash, captured_at
    ) VALUES %s
    ON CONFLICT (company_id, job_key, snapshot_date, country_norm, COALESCE(city_norm, ''))
    DO UPDATE SET
      title = EXCLUDED.title,
      snapshot_month = EXCLUDED.snapshot_month,
      city_raw = EXCLUDED.city_raw,
      country_raw = EXCLUDED.country_raw,
      location_raw = EXCLUDED.location_raw,
      region_norm = EXCLUDED.region_norm,
      location_confidence = EXCLUDED.location_confidence,
      posted_at = EXCLUDED.posted_at,
      job_hash = EXCLUDED.job_hash,
      captured_at = EXCLUDED.captured_at,
      updated_at = now();
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, safe_rows, page_size=500)


def refresh_mv_country_month_counts():
    """
    Backward-compatible name.
    Refreshes both trend MVs when present:
      - mv_country_month_counts
      - mv_company_country_month_counts
    """
    mv_names = (
        "mv_country_month_counts",
        "mv_company_country_month_counts",
        "mv_country_month_avg_counts",
        "mv_company_country_month_avg_counts",
    )
    for mv_name in mv_names:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"REFRESH MATERIALIZED VIEW {mv_name};")
        except psycopg2.Error as e:
            # Keep ingestion robust before migrations are fully applied.
            print(f"[refresh_mv_country_month_counts] skipped {mv_name}: {e}", file=sys.stderr)
