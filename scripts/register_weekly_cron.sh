#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
COMPANIES="${2:-nvidia,nokia}"
CRON_SCHEDULE="${3:-0 3 * * 0}" # Every Sunday 03:00

RUNNER="$PROJECT_ROOT/scripts/run_weekly_ingest.sh"
LOG_DIR="$PROJECT_ROOT/logs/ingest"
LOG_FILE="$LOG_DIR/weekly_cron.log"

mkdir -p "$LOG_DIR"

CRON_CMD="cd \"$PROJECT_ROOT\" && \"$RUNNER\" \"$PROJECT_ROOT\" \"$COMPANIES\" >> \"$LOG_FILE\" 2>&1"
CRON_LINE="$CRON_SCHEDULE $CRON_CMD"

TMP_CRON="$(mktemp)"
crontab -l 2>/dev/null | grep -v 'run_weekly_ingest.sh' > "$TMP_CRON" || true
echo "$CRON_LINE" >> "$TMP_CRON"
crontab "$TMP_CRON"
rm -f "$TMP_CRON"

echo "Cron installed:"
echo "$CRON_LINE"
