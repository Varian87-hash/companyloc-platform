-- 0003_weekly_snapshots_monthly_avg.sql
-- Support weekly snapshots while keeping monthly query granularity.

BEGIN;

-- 1) Keep per-run history by adding snapshot_date.
ALTER TABLE job_location_facts
  ADD COLUMN IF NOT EXISTS snapshot_date DATE;

-- Backfill existing rows (best effort).
UPDATE job_location_facts
SET snapshot_date = COALESCE(snapshot_date, (captured_at AT TIME ZONE 'UTC')::date, snapshot_month)
WHERE snapshot_date IS NULL;

ALTER TABLE job_location_facts
  ALTER COLUMN snapshot_date SET NOT NULL;

-- 2) Replace idempotency key: one row per company/job/date/location.
DROP INDEX IF EXISTS ux_job_loc_facts_idem;
DROP INDEX IF EXISTS uq_job_location_facts_monthly_loc;

CREATE UNIQUE INDEX IF NOT EXISTS ux_job_loc_facts_daily_loc
  ON job_location_facts (company_id, job_key, snapshot_date, country_norm, COALESCE(city_norm, ''));

CREATE INDEX IF NOT EXISTS ix_job_loc_facts_company_snapshot_date
  ON job_location_facts (company_id, snapshot_date);

CREATE INDEX IF NOT EXISTS ix_job_loc_facts_country_snapshot_date
  ON job_location_facts (country_norm, snapshot_date);

COMMIT;

-- 3) Monthly averages from weekly/daily snapshots.
DROP MATERIALIZED VIEW IF EXISTS mv_country_month_avg_counts;
DROP MATERIALIZED VIEW IF EXISTS mv_company_country_month_avg_counts;

CREATE MATERIALIZED VIEW mv_country_month_avg_counts AS
SELECT
  date_trunc('month', t.snapshot_date)::date AS snapshot_month,
  t.country_norm,
  AVG(t.jobs_count)::double precision AS jobs_count,
  COUNT(*)::int AS sample_points
FROM (
  SELECT
    snapshot_date,
    country_norm,
    COUNT(DISTINCT (company_id, job_key))::int AS jobs_count
  FROM job_location_facts
  WHERE country_norm <> 'UN'
  GROUP BY snapshot_date, country_norm
) t
GROUP BY date_trunc('month', t.snapshot_date)::date, t.country_norm;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_country_month_avg_counts
  ON mv_country_month_avg_counts (snapshot_month, country_norm);

CREATE MATERIALIZED VIEW mv_company_country_month_avg_counts AS
SELECT
  t.company_id,
  date_trunc('month', t.snapshot_date)::date AS snapshot_month,
  t.country_norm,
  AVG(t.jobs_count)::double precision AS jobs_count,
  COUNT(*)::int AS sample_points
FROM (
  SELECT
    company_id,
    snapshot_date,
    country_norm,
    COUNT(DISTINCT job_key)::int AS jobs_count
  FROM job_location_facts
  WHERE country_norm <> 'UN'
  GROUP BY company_id, snapshot_date, country_norm
) t
GROUP BY t.company_id, date_trunc('month', t.snapshot_date)::date, t.country_norm;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_company_country_month_avg_counts
  ON mv_company_country_month_avg_counts (company_id, snapshot_month, country_norm);
