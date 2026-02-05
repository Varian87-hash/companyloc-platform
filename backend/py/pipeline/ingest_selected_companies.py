import importlib
import traceback

from backend.py.pipeline.config import DEFAULT_COMPANIES as DEFAULT_ORDER
from backend.py.pipeline.config import PIPELINE_MODULES


def run_one(company_key: str) -> bool:
    mod_name = PIPELINE_MODULES.get(company_key)
    if not mod_name:
        print(f"[SKIP] {company_key}: pipeline not implemented yet")
        return False

    mod = importlib.import_module(mod_name)
    if not hasattr(mod, "main"):
        print(f"[SKIP] {company_key}: module has no main()")
        return False
    if getattr(mod, "PIPELINE_STATUS", "ok") == "skip":
        reason = getattr(mod, "PIPELINE_SKIP_REASON", "pipeline_skipped")
        print(f"[SKIP] {company_key}: {reason}")
        return False

    print(f"[RUN ] {company_key}")
    mod.main()
    print(f"[DONE] {company_key}")
    return True


def main():
    ok = 0
    fail = 0
    skip = 0

    for company_key in DEFAULT_ORDER:
        try:
            done = run_one(company_key)
            if done:
                ok += 1
            else:
                skip += 1
        except Exception:  # noqa: BLE001
            fail += 1
            print(f"[FAIL] {company_key}")
            traceback.print_exc()

    print(f"finished: ok={ok} skip={skip} fail={fail}")


if __name__ == "__main__":
    main()
