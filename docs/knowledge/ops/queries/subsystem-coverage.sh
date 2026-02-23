#!/usr/bin/env bash
# subsystem-coverage.sh — Entry count by subsystem
set -euo pipefail

VAULT="docs/knowledge"
SUBSYSTEMS=(ingestion graphql data-layer signing services auth)

echo "Entry coverage by subsystem"
echo ""

TOTAL=0
for subsystem in "${SUBSYSTEMS[@]}"; do
  COUNT=$(grep -rl "^subsystem: ${subsystem}" "$VAULT/entries" 2>/dev/null | wc -l | tr -d ' ')
  TOTAL=$((TOTAL + COUNT))
  printf "  %-20s %3d entries\n" "$subsystem" "$COUNT"
done

echo ""
echo "  Total entries: $TOTAL"
echo ""

# Also show entry type breakdown
echo "Entry type breakdown"
echo ""
for type in decision insight pattern gotcha; do
  COUNT=$(grep -rl "^type: ${type}" "$VAULT/entries" 2>/dev/null | wc -l | tr -d ' ')
  printf "  %-20s %3d\n" "$type" "$COUNT"
done
