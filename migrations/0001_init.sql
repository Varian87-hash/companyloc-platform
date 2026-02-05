-- 0001_init.sql
-- Core tables for company metadata, monthly job-location facts, and alias normalization.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS companies (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL UNIQUE,
    careers_url text,
    source_type text NOT NULL DEFAULT 'custom',
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS location_aliases (
    id bigserial PRIMARY KEY,
    raw_text text NOT NULL UNIQUE,
    city_norm text,
    region_norm text,
    country_norm text NOT NULL,
    confidence double precision NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
    source text NOT NULL DEFAULT 'manual',
    notes text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_location_aliases_country_norm
    ON location_aliases(country_norm);

CREATE TABLE IF NOT EXISTS job_location_facts (
    id bigserial PRIMARY KEY,
    company_id uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    job_key text NOT NULL,
    snapshot_month date NOT NULL,

    title text,
    team text,
    level text,
    employment_type text,

    city_raw text,
    country_raw text,
    location_raw text,

    city_norm text,
    region_norm text,
    country_norm text NOT NULL,
    location_confidence double precision NOT NULL DEFAULT 0 CHECK (location_confidence >= 0 AND location_confidence <= 1),

    posted_at text,
    updated_at_raw text,

    captured_at timestamptz NOT NULL DEFAULT now(),
    job_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Idempotency: one row per company/job/month/country/city-key.
CREATE UNIQUE INDEX IF NOT EXISTS uq_job_location_facts_monthly_loc
    ON job_location_facts (
        company_id,
        job_key,
        snapshot_month,
        country_norm,
        COALESCE(city_norm, '')
    );

CREATE INDEX IF NOT EXISTS idx_job_location_facts_company_month
    ON job_location_facts(company_id, snapshot_month);

CREATE INDEX IF NOT EXISTS idx_job_location_facts_country_month
    ON job_location_facts(country_norm, snapshot_month);

CREATE INDEX IF NOT EXISTS idx_job_location_facts_snapshot_month
    ON job_location_facts(snapshot_month);
