# CompanyLoc Platform

Lightweight ingestion + API + dashboard for company hiring location trends.

## Quick Start

1. Create and activate virtual environment.
2. Install dependencies.
3. Set `NEON_DATABASE_URL` in environment (or `.env` for local only).
4. Run API:

```powershell
.\.venv\Scripts\python -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```

Open: `http://127.0.0.1:8000/app`

## Ingestion

Run weekly ingestion for default companies:

```powershell
.\.venv\Scripts\python -u -m backend.py.pipeline.ingest_weekly
```

Run selected companies:

```powershell
.\.venv\Scripts\python -u -m backend.py.pipeline.ingest_weekly --companies amazon,apple
```

Linux shell runner:

```bash
bash scripts/run_weekly_ingest.sh /path/to/companyloc-platform "amazon,apple"
```

Register weekly cron job on Linux:

```bash
bash scripts/register_weekly_cron.sh /path/to/companyloc-platform "amazon,apple" "0 3 * * 0"
```

## Tests

```powershell
.\.venv\Scripts\python -m unittest tests.test_api_validation -v
```

## Deployment Notes

- Configure `NEON_DATABASE_URL` from server environment (recommended), not from local `.env`.
- Current scheduling scripts in `scripts/*.ps1` are Windows-oriented.
- If deploying on Linux, prefer `cron` + shell wrapper for ingestion.
