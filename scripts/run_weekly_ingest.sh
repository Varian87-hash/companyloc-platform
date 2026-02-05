#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
COMPANIES="${2:-nvidia,nokia}"

cd "$PROJECT_ROOT"

if [[ -x ".venv/bin/python" ]]; then
  exec .venv/bin/python -u -m backend.py.pipeline.ingest_weekly --companies "$COMPANIES"
fi

exec python3 -u -m backend.py.pipeline.ingest_weekly --companies "$COMPANIES"
