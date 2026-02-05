from datetime import date
import logging
from pathlib import Path
from typing import Optional
from uuid import UUID

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

from backend.py.storage.neon import get_conn

load_dotenv(".env")

app = FastAPI(title="CompanyLoc Read API", version="0.1.0")
logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent
DASHBOARD_FILE = BASE_DIR / "static" / "dashboard.html"


class HealthResponse(BaseModel):
    ok: bool


class CompanyItem(BaseModel):
    id: UUID
    name: str
    careers_url: Optional[str] = None
    source_type: str
    is_active: bool


class CompaniesResponse(BaseModel):
    items: list[CompanyItem]


class CityJobsItem(BaseModel):
    city_norm: str
    jobs_count: int


class CountryJobsItem(BaseModel):
    country_norm: str
    jobs_count: int
    cities: list[CityJobsItem]


class CurrentLocationsResponse(BaseModel):
    company: CompanyItem
    snapshot_month: Optional[date] = None
    remote_jobs_count: int = 0
    countries: list[CountryJobsItem]


class CountryTrendItem(BaseModel):
    snapshot_month: date
    country_norm: str
    jobs_count: float
    sample_points: Optional[int] = None


class CountryTrendResponse(BaseModel):
    company_id: Optional[UUID] = None
    country: Optional[str] = None
    from_month: Optional[date] = None
    to_month: Optional[date] = None
    items: list[CountryTrendItem]


def _parse_month(v: Optional[str]) -> Optional[date]:
    if v is None:
        return None
    try:
        y, m = v.split("-")
        return date(int(y), int(m), 1)
    except (TypeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"invalid month: {v}, expected YYYY-MM") from e


def _validate_month_range(from_dt: Optional[date], to_dt: Optional[date]) -> None:
    if from_dt and to_dt and from_dt > to_dt:
        raise HTTPException(
            status_code=400,
            detail=f"invalid range: from ({from_dt.isoformat()}) must be <= to ({to_dt.isoformat()})",
        )


def _company_exists(company_id: UUID) -> dict:
    sql = """
    SELECT id, name, careers_url, source_type, is_active
    FROM companies
    WHERE id = %s
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (str(company_id),))
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="company not found")
    return dict(row)


@app.get("/healthz", response_model=HealthResponse)
def healthz():
    return {"ok": True}


@app.get("/app")
def dashboard_page():
    if not DASHBOARD_FILE.exists():
        raise HTTPException(status_code=404, detail="dashboard not found")
    return FileResponse(DASHBOARD_FILE)


@app.get("/v1/companies", response_model=CompaniesResponse)
def list_companies(active_only: bool = True):
    sql = """
    SELECT id, name, careers_url, source_type, is_active
    FROM companies
    WHERE (%s = FALSE OR is_active = TRUE)
    ORDER BY name
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (active_only,))
            rows = cur.fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/v1/companies/{company_id}/locations/current", response_model=CurrentLocationsResponse)
def company_current_locations(company_id: UUID):
    company = _company_exists(company_id)

    latest_sql = """
    SELECT
      MAX(snapshot_date) AS snapshot_date,
      date_trunc('month', MAX(snapshot_date))::date AS snapshot_month
    FROM job_location_facts
    WHERE company_id = %s
    """
    fallback_latest_sql = """
    SELECT MAX(snapshot_month) AS snapshot_month
    FROM job_location_facts
    WHERE company_id = %s
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(latest_sql, (str(company_id),))
                latest = cur.fetchone()
                snapshot_month = latest["snapshot_month"] if latest else None
                snapshot_date = latest["snapshot_date"] if latest else None
            except psycopg2.Error as e:
                logger.warning("latest snapshot query fallback for %s: %s", company_id, e)
                cur.execute(fallback_latest_sql, (str(company_id),))
                latest = cur.fetchone()
                snapshot_month = latest["snapshot_month"] if latest else None
                snapshot_date = None
    if snapshot_month is None:
        return {
            "company": company,
            "snapshot_month": None,
            "countries": [],
        }

    countries_sql = """
    SELECT
      country_norm,
      COUNT(DISTINCT job_key) AS jobs_count
    FROM job_location_facts
    WHERE company_id = %s
      AND snapshot_date = %s
    GROUP BY country_norm
    ORDER BY jobs_count DESC, country_norm
    """
    fallback_countries_sql = """
    SELECT
      country_norm,
      COUNT(DISTINCT job_key) AS jobs_count
    FROM job_location_facts
    WHERE company_id = %s
      AND snapshot_month = %s
    GROUP BY country_norm
    ORDER BY jobs_count DESC, country_norm
    """
    cities_sql = """
    SELECT
      country_norm,
      city_norm,
      COUNT(DISTINCT job_key) AS jobs_count
    FROM job_location_facts
    WHERE company_id = %s
      AND snapshot_date = %s
    GROUP BY country_norm, city_norm
    ORDER BY country_norm, jobs_count DESC, city_norm
    """
    fallback_cities_sql = """
    SELECT
      country_norm,
      city_norm,
      COUNT(DISTINCT job_key) AS jobs_count
    FROM job_location_facts
    WHERE company_id = %s
      AND snapshot_month = %s
    GROUP BY country_norm, city_norm
    ORDER BY country_norm, jobs_count DESC, city_norm
    """
    remote_sql = """
    SELECT COUNT(DISTINCT job_key) AS remote_jobs_count
    FROM job_location_facts
    WHERE company_id = %s
      AND snapshot_date = %s
      AND (
        LOWER(COALESCE(location_raw, '')) LIKE '%%remote%%'
        OR LOWER(COALESCE(city_norm, '')) LIKE '%%remote%%'
      )
    """
    fallback_remote_sql = """
    SELECT COUNT(DISTINCT job_key) AS remote_jobs_count
    FROM job_location_facts
    WHERE company_id = %s
      AND snapshot_month = %s
      AND (
        LOWER(COALESCE(location_raw, '')) LIKE '%%remote%%'
        OR LOWER(COALESCE(city_norm, '')) LIKE '%%remote%%'
      )
    """

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(countries_sql, (str(company_id), snapshot_date))
                country_rows = [dict(r) for r in cur.fetchall()]
                cur.execute(cities_sql, (str(company_id), snapshot_date))
                city_rows = [dict(r) for r in cur.fetchall()]
                cur.execute(remote_sql, (str(company_id), snapshot_date))
                remote_row = cur.fetchone() or {"remote_jobs_count": 0}
            except psycopg2.Error as e:
                logger.warning("current locations query fallback for %s: %s", company_id, e)
                cur.execute(fallback_countries_sql, (str(company_id), snapshot_month))
                country_rows = [dict(r) for r in cur.fetchall()]
                cur.execute(fallback_cities_sql, (str(company_id), snapshot_month))
                city_rows = [dict(r) for r in cur.fetchall()]
                cur.execute(fallback_remote_sql, (str(company_id), snapshot_month))
                remote_row = cur.fetchone() or {"remote_jobs_count": 0}

    cities_by_country: dict[str, list[dict]] = {}
    for r in city_rows:
        c = r["country_norm"]
        city = r["city_norm"] if r["city_norm"] is not None else "UNKNOWN"
        cities_by_country.setdefault(c, []).append(
            {
                "city_norm": city,
                "jobs_count": r["jobs_count"],
            }
        )

    countries = []
    for r in country_rows:
        countries.append(
            {
                "country_norm": r["country_norm"],
                "jobs_count": r["jobs_count"],
                "cities": cities_by_country.get(r["country_norm"], []),
            }
        )

    return {
        "company": company,
        "snapshot_month": snapshot_month,
        "remote_jobs_count": int(remote_row.get("remote_jobs_count") or 0),
        "countries": countries,
    }


@app.get("/v1/trends/countries", response_model=CountryTrendResponse)
def trend_countries(
    company_id: Optional[UUID] = Query(default=None),
    country: Optional[str] = Query(default=None, pattern="^[A-Za-z]{2}$"),
    from_month: Optional[str] = Query(default=None, alias="from"),
    to_month: Optional[str] = Query(default=None, alias="to"),
):
    from_dt = _parse_month(from_month)
    to_dt = _parse_month(to_month)
    _validate_month_range(from_dt, to_dt)
    country_norm = country.upper() if country else None

    if country_norm == "UN":
        if company_id:
            _company_exists(company_id)
            sql = """
            WITH daily AS (
              SELECT
                snapshot_date,
                COUNT(DISTINCT job_key)::int AS jobs_count
              FROM job_location_facts
              WHERE company_id = %s
                AND country_norm = 'UN'
              GROUP BY snapshot_date
            ),
            monthly AS (
              SELECT
                date_trunc('month', snapshot_date)::date AS snapshot_month,
                'UN'::text AS country_norm,
                AVG(jobs_count)::double precision AS jobs_count,
                COUNT(*)::int AS sample_points
              FROM daily
              GROUP BY date_trunc('month', snapshot_date)::date
            )
            SELECT snapshot_month, country_norm, jobs_count, sample_points
            FROM monthly
            WHERE (%s::date IS NULL OR snapshot_month >= %s::date)
              AND (%s::date IS NULL OR snapshot_month <= %s::date)
            ORDER BY snapshot_month
            """
            params = (str(company_id), from_dt, from_dt, to_dt, to_dt)
        else:
            sql = """
            WITH daily AS (
              SELECT
                snapshot_date,
                COUNT(DISTINCT (company_id, job_key))::int AS jobs_count
              FROM job_location_facts
              WHERE country_norm = 'UN'
              GROUP BY snapshot_date
            ),
            monthly AS (
              SELECT
                date_trunc('month', snapshot_date)::date AS snapshot_month,
                'UN'::text AS country_norm,
                AVG(jobs_count)::double precision AS jobs_count,
                COUNT(*)::int AS sample_points
              FROM daily
              GROUP BY date_trunc('month', snapshot_date)::date
            )
            SELECT snapshot_month, country_norm, jobs_count, sample_points
            FROM monthly
            WHERE (%s::date IS NULL OR snapshot_month >= %s::date)
              AND (%s::date IS NULL OR snapshot_month <= %s::date)
            ORDER BY snapshot_month
            """
            params = (from_dt, from_dt, to_dt, to_dt)

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = [dict(r) for r in cur.fetchall()]
        return {
            "company_id": company_id,
            "country": country_norm,
            "from_month": from_dt,
            "to_month": to_dt,
            "items": rows,
        }

    if company_id:
        _company_exists(company_id)
        sql = """
        SELECT snapshot_month, country_norm, jobs_count, sample_points
        FROM mv_company_country_month_avg_counts
        WHERE company_id = %s
          AND (%s::date IS NULL OR snapshot_month >= %s::date)
          AND (%s::date IS NULL OR snapshot_month <= %s::date)
          AND (%s IS NULL OR country_norm = %s)
        ORDER BY snapshot_month, country_norm
        """
        fallback_sql = """
        SELECT snapshot_month, country_norm, jobs_count::double precision AS jobs_count, NULL::int AS sample_points
        FROM mv_company_country_month_counts
        WHERE company_id = %s
          AND (%s::date IS NULL OR snapshot_month >= %s::date)
          AND (%s::date IS NULL OR snapshot_month <= %s::date)
          AND (%s IS NULL OR country_norm = %s)
        ORDER BY snapshot_month, country_norm
        """
        params = (str(company_id), from_dt, from_dt, to_dt, to_dt, country_norm, country_norm)
    else:
        sql = """
        SELECT snapshot_month, country_norm, jobs_count, sample_points
        FROM mv_country_month_avg_counts
        WHERE (%s::date IS NULL OR snapshot_month >= %s::date)
          AND (%s::date IS NULL OR snapshot_month <= %s::date)
          AND (%s IS NULL OR country_norm = %s)
        ORDER BY snapshot_month, country_norm
        """
        fallback_sql = """
        SELECT snapshot_month, country_norm, jobs_count::double precision AS jobs_count, NULL::int AS sample_points
        FROM mv_country_month_counts
        WHERE (%s::date IS NULL OR snapshot_month >= %s::date)
          AND (%s::date IS NULL OR snapshot_month <= %s::date)
          AND (%s IS NULL OR country_norm = %s)
        ORDER BY snapshot_month, country_norm
        """
        params = (from_dt, from_dt, to_dt, to_dt, country_norm, country_norm)

    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                try:
                    cur.execute(sql, params)
                    rows = [dict(r) for r in cur.fetchall()]
                except psycopg2.Error as e:
                    logger.warning("trend query fallback to legacy MV (company_id=%s): %s", company_id, e)
                    cur.execute(fallback_sql, params)
                    rows = [dict(r) for r in cur.fetchall()]
    except psycopg2.Error as e:
        logger.exception("trend query failed (company_id=%s, country=%s)", company_id, country_norm)
        raise HTTPException(
            status_code=500,
            detail=f"trend query failed, check MV migrations: {e}",
        ) from e

    return {
        "company_id": company_id,
        "country": country_norm,
        "from_month": from_dt,
        "to_month": to_dt,
        "items": rows,
    }
