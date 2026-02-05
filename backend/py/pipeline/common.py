import hashlib
import json
from typing import Optional

from backend.py.storage.neon import get_conn

COUNTRY_NAME_TO_ISO2 = {
    "UNITED STATES": "US",
    "USA": "US",
    "U.S.": "US",
    "U.S.A.": "US",
    "CANADA": "CA",
    "MEXICO": "MX",
    "GERMANY": "DE",
    "FRANCE": "FR",
    "UNITED KINGDOM": "GB",
    "UK": "GB",
    "IRELAND": "IE",
    "NETHERLANDS": "NL",
    "BELGIUM": "BE",
    "SWITZERLAND": "CH",
    "SPAIN": "ES",
    "ITALY": "IT",
    "POLAND": "PL",
    "CZECH REPUBLIC": "CZ",
    "CZECHIA": "CZ",
    "ROMANIA": "RO",
    "SWEDEN": "SE",
    "NORWAY": "NO",
    "DENMARK": "DK",
    "FINLAND": "FI",
    "ISRAEL": "IL",
    "INDIA": "IN",
    "SINGAPORE": "SG",
    "TAIWAN": "TW",
    "JAPAN": "JP",
    "KOREA": "KR",
    "SOUTH KOREA": "KR",
    "CHINA": "CN",
    "HONG KONG": "HK",
    "AUSTRALIA": "AU",
    "NEW ZEALAND": "NZ",
    "BRAZIL": "BR",
}


def as_text(v):
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    if isinstance(v, dict):
        for k in ("descriptor", "label", "value", "text", "name"):
            x = v.get(k)
            if isinstance(x, str) and x.strip():
                return x.strip()
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def normalize_country_iso2(v: Optional[str]) -> str:
    s = as_text(v)
    if not s:
        return "UN"
    s = s.strip()
    if len(s) == 2 and s.isalpha():
        return s.upper()
    return COUNTRY_NAME_TO_ISO2.get(s.upper(), "UN")


def get_company_id_by_name(name: str) -> str:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM companies WHERE name = %s", (name,))
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"company not found: {name}")
            return row[0]


def stable_hash(company_id, job_key, snapshot_month, title, locations):
    payload = f"{company_id}|{job_key}|{snapshot_month}|{title}|{sorted(locations)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def dedup_rows_by_confidence(rows: list[tuple]) -> list[tuple]:
    dedup = {}
    for row in rows:
        company_id = as_text(row[0]) or ""
        job_key = as_text(row[1]) or ""
        snapshot_date = row[3]
        country_norm = normalize_country_iso2(row[10])
        city_norm = as_text(row[8]) or ""
        key = (company_id, job_key, snapshot_date, country_norm, city_norm)
        conf = float(row[11] or 0.0)
        prev = dedup.get(key)
        if prev is None or conf > float(prev[11] or 0.0):
            dedup[key] = row
    return list(dedup.values())
