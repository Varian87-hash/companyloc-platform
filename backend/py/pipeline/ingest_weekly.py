import argparse
import contextlib
import importlib
import io
import json
import math
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from backend.py.pipeline.config import DEFAULT_COMPANIES, PIPELINE_MODULES

COMPANY_NAME_MAP = {
    "amazon": "Amazon",
    "apple": "Apple",
    "google": "Google",
    "intel": "Intel",
    "meta": "Meta",
    "microsoft": "Microsoft",
    "nvidia": "NVIDIA",
    "nokia": "Nokia",
}
MIN_FETCHED_BY_COMPANY = {
    "amazon": 1,
    "apple": 1,
    "google": 1,
    "intel": 1,
    "meta": 1,
    "microsoft": 1,
    "nvidia": 1,
    "nokia": 1,
}
MIN_FETCH_RATIO_BY_COMPANY = {
    "meta": 0.7,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_int(pattern: str, text: str) -> int | None:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _parse_metrics(text: str) -> dict:
    return {
        "source": (re.search(r"\bsource:\s*(.+)", text, flags=re.IGNORECASE) or [None, None])[1],
        "total": _extract_int(r"\btotal:\s*(\d+)", text),
        "fetched": _extract_int(r"Fetched postings:\s*(\d+)", text),
        "inserted_or_updated": _extract_int(r"Inserted/updated rows:\s*(\d+)", text),
    }


def _fetch_country_topn(company_key: str, limit: int = 5) -> list[dict]:
    try:
        from backend.py.storage.neon import get_conn
    except Exception:
        return []

    company_name = COMPANY_NAME_MAP.get(company_key, company_key)
    sql = """
        SELECT f.country_norm, COUNT(DISTINCT f.job_key) AS job_count
        FROM job_location_facts f
        JOIN companies c ON c.id = f.company_id
        WHERE LOWER(c.name) = LOWER(%s)
          AND f.snapshot_date = (
              SELECT MAX(f2.snapshot_date)
              FROM job_location_facts f2
              WHERE f2.company_id = f.company_id
          )
        GROUP BY f.country_norm
        ORDER BY job_count DESC, f.country_norm ASC
        LIMIT %s
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (company_name, limit))
                rows = cur.fetchall() or []
        return [{"country": r[0], "jobs": int(r[1])} for r in rows]
    except Exception:
        return []


def _run_one(company_key: str, enforce_quality_gate: bool = True) -> dict:
    started = _now_iso()
    t0 = datetime.now(timezone.utc)
    try:
        mod_name = PIPELINE_MODULES.get(company_key)
        if not mod_name:
            return {
                "company": company_key,
                "status": "skip",
                "reason": "pipeline_not_implemented",
                "started_at": started,
                "ended_at": _now_iso(),
                "duration_sec": 0.0,
            }

        mod = importlib.import_module(mod_name)
        if not hasattr(mod, "main"):
            return {
                "company": company_key,
                "status": "skip",
                "reason": "missing_main",
                "started_at": started,
                "ended_at": _now_iso(),
                "duration_sec": 0.0,
            }

        pipeline_status = getattr(mod, "PIPELINE_STATUS", "ok")
        if pipeline_status == "skip":
            return {
                "company": company_key,
                "status": "skip",
                "reason": getattr(mod, "PIPELINE_SKIP_REASON", "pipeline_skipped"),
                "started_at": started,
                "ended_at": _now_iso(),
                "duration_sec": 0.0,
            }

        print(f"[RUN ] {company_key}")
        out = io.StringIO()
        err = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            mod.main()

        captured_out = out.getvalue()
        captured_err = err.getvalue()
        if captured_out:
            print(captured_out, end="" if captured_out.endswith("\n") else "\n")
        if captured_err:
            print(captured_err, end="" if captured_err.endswith("\n") else "\n", file=sys.stderr)

        metrics = _parse_metrics(captured_out + "\n" + captured_err)
        min_fetched = MIN_FETCHED_BY_COMPANY.get(company_key, 1)
        min_ratio = MIN_FETCH_RATIO_BY_COMPANY.get(company_key)
        gate_failure_reasons: list[str] = []
        fetched = metrics.get("fetched")
        total = metrics.get("total")
        if enforce_quality_gate and fetched is not None and fetched < min_fetched:
            gate_failure_reasons.append(f"fetched_below_threshold: fetched={fetched} min_required={min_fetched}")
        if (
            enforce_quality_gate
            and min_ratio is not None
            and isinstance(total, int)
            and total > 0
            and fetched is not None
        ):
            min_required_by_ratio = math.ceil(total * float(min_ratio))
            if fetched < min_required_by_ratio:
                gate_failure_reasons.append(
                    (
                        "fetched_coverage_below_threshold: "
                        f"fetched={fetched} total={total} "
                        f"coverage={fetched / total:.3f} min_coverage={float(min_ratio):.3f} "
                        f"min_required={min_required_by_ratio}"
                    )
                )

        gate_failure = "; ".join(gate_failure_reasons) if gate_failure_reasons else None

        if gate_failure:
            dt = (datetime.now(timezone.utc) - t0).total_seconds()
            print(f"[FAIL] {company_key}: {gate_failure}")
            return {
                "company": company_key,
                "status": "fail",
                "reason": gate_failure,
                "metrics": metrics,
                "country_top5": _fetch_country_topn(company_key),
                "started_at": started,
                "ended_at": _now_iso(),
                "duration_sec": round(dt, 3),
            }

        dt = (datetime.now(timezone.utc) - t0).total_seconds()
        print(f"[DONE] {company_key} ({dt:.1f}s)")
        return {
            "company": company_key,
            "status": "ok",
            "metrics": metrics,
            "country_top5": _fetch_country_topn(company_key),
            "started_at": started,
            "ended_at": _now_iso(),
            "duration_sec": round(dt, 3),
        }
    except Exception as e:  # noqa: BLE001
        dt = (datetime.now(timezone.utc) - t0).total_seconds()
        print(f"[FAIL] {company_key}: {type(e).__name__}: {e}")
        traceback.print_exc()
        return {
            "company": company_key,
            "status": "fail",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "started_at": started,
            "ended_at": _now_iso(),
            "duration_sec": round(dt, 3),
        }


def _write_run_log(run_result: dict, log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = log_dir / f"weekly_ingest_{ts}.json"
    path.write_text(json.dumps(run_result, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run weekly ingestion for selected companies.")
    p.add_argument(
        "--companies",
        default=",".join(DEFAULT_COMPANIES),
        help="Comma-separated company keys, e.g. nvidia,nokia",
    )
    p.add_argument(
        "--log-dir",
        default="logs/ingest",
        help="Directory for run summary JSON logs.",
    )
    p.add_argument(
        "--no-quality-gate",
        action="store_true",
        help="Disable minimum fetched-postings quality gate.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv()

    companies = [x.strip().lower() for x in args.companies.split(",") if x.strip()]
    if not companies:
        print("No companies selected.")
        return 2

    run_started = _now_iso()
    results = [_run_one(company_key, enforce_quality_gate=not args.no_quality_gate) for company_key in companies]
    ok = sum(1 for r in results if r["status"] == "ok")
    skip = sum(1 for r in results if r["status"] == "skip")
    fail = sum(1 for r in results if r["status"] == "fail")
    exit_code = 1 if fail > 0 else 0

    summary = {
        "run_started_at": run_started,
        "run_ended_at": _now_iso(),
        "companies": companies,
        "ok": ok,
        "skip": skip,
        "fail": fail,
        "exit_code": exit_code,
        "results": results,
    }

    log_path = _write_run_log(summary, Path(args.log_dir))
    print(f"finished: ok={ok} skip={skip} fail={fail} exit_code={exit_code}")
    print(f"log: {log_path}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
