#!/usr/bin/env bash
# orphan-entries.sh — Find entries with no incoming wiki-links
set -euo pipefail

VAULT="docs/knowledge"

echo "Orphan entries (no incoming wiki-links)"
echo ""

find "$VAULT/entries" -name "*.md" | while read -r file; do
  BASENAME=$(basename "$file" .md)
  # Count incoming links from all vault files
  LINK_COUNT=$(grep -r "\[\[.*${BASENAME}.*\]\]" "$VAULT" --include="*.md" --exclude="$file" -l 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$LINK_COUNT" -eq 0 ]]; then
    echo "  ORPHAN: $file"
  fi
done
