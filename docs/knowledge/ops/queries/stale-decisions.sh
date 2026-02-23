#!/usr/bin/env bash
# stale-decisions.sh — Find entries with status: current older than 90 days
set -euo pipefail

VAULT="docs/knowledge"
THRESHOLD_DAYS=90
THRESHOLD_DATE=$(date -d "-${THRESHOLD_DAYS} days" +%Y-%m-%d 2>/dev/null || date -v-${THRESHOLD_DAYS}d +%Y-%m-%d)

echo "Entries with status: current older than $THRESHOLD_DAYS days"
echo "Threshold date: $THRESHOLD_DATE"
echo ""

find "$VAULT/entries" -name "*.md" | while read -r file; do
  # Check if file has status: current
  if grep -q "^status: current" "$file"; then
    # Get the file modification date
    MOD_DATE=$(git log -1 --format="%ai" -- "$file" 2>/dev/null | cut -d' ' -f1 || date -r "$file" +%Y-%m-%d 2>/dev/null || echo "unknown")
    if [[ "$MOD_DATE" != "unknown" ]] && [[ "$MOD_DATE" < "$THRESHOLD_DATE" ]]; then
      SUBSYSTEM=$(grep "^subsystem:" "$file" | cut -d' ' -f2 || echo "unknown")
      echo "  $MOD_DATE | $SUBSYSTEM | $file"
    fi
  fi
done
