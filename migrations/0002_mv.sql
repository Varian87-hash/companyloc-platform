-- 0002_mv.sql
-- Trend materialized views.

-- Global country-month trend across all companies.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_country_month_counts AS
SELECT
    jlf.snapshot_month,
    jlf.country_norm,
    COUNT(DISTINCT (jlf.company_id, jlf.job_key)) AS jobs_count
FROM job_location_facts jlf
WHERE jlf.country_norm <> 'UN'
GROUP BY
    jlf.snapshot_month,
    jlf.country_norm;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_country_month_counts
    ON mv_country_month_counts(snapshot_month, country_norm);

-- Company-country-month trend.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_company_country_month_counts AS
SELECT
    jlf.company_id,
    jlf.snapshot_month,
    jlf.country_norm,
    COUNT(DISTINCT jlf.job_key) AS jobs_count
FROM job_location_facts jlf
WHERE jlf.country_norm <> 'UN'
GROUP BY
    jlf.company_id,
    jlf.snapshot_month,
    jlf.country_norm;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_company_country_month_counts
    ON mv_company_country_month_counts(company_id, snapshot_month, country_norm);
