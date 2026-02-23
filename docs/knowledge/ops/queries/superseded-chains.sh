#!/usr/bin/env bash
# superseded-chains.sh — Trace decision supersession chains
set -euo pipefail

VAULT="docs/knowledge"

echo "Supersession chains"
echo ""

find "$VAULT/entries" -name "*.md" | while read -r file; do
  if grep -q "^superseded_by:" "$file"; then
    SUPERSEDED_BY=$(grep "^superseded_by:" "$file" | cut -d' ' -f2-)
    STATUS=$(grep "^status:" "$file" | cut -d' ' -f2)
    echo "  $(basename $file) [${STATUS}] → ${SUPERSEDED_BY}"
  fi
done
