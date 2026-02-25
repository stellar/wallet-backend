#!/usr/bin/env bash
# stale-references.sh — Report reference docs with accumulated source code changes
# A doc with >= 5 changed source files since last update is considered stale.
# Run: bash docs/knowledge/ops/queries/stale-references.sh
set -euo pipefail

CHANGES_FILE="docs/knowledge/ops/reference-doc-changes.yaml"
REFERENCES_DIR="docs/knowledge/references"
THRESHOLD=5

if [[ ! -f "$CHANGES_FILE" ]]; then
  echo "Error: $CHANGES_FILE not found. Run from the wallet-backend root." >&2
  exit 1
fi

echo "Reference Doc Staleness Report"
echo "Threshold: $THRESHOLD changed source files"
echo "════════════════════════════════════════"
echo ""

STALE_COUNT=0
TOTAL_COUNT=0

# Parse YAML manually (no yq dependency required)
# Format: key: { count: N, files: [...] }
while IFS= read -r line; do
  # Match lines like: "  ingestion-pipeline: { count: 5, files: [...] }"
  if [[ "$line" =~ ^[[:space:]]+([a-z-]+):[[:space:]]+\{[[:space:]]*count:[[:space:]]*([0-9]+) ]]; then
    DOC_KEY="${BASH_REMATCH[1]}"
    COUNT="${BASH_REMATCH[2]}"
    TOTAL_COUNT=$((TOTAL_COUNT + 1))

    # Map doc key to reference file
    REF_FILE="$REFERENCES_DIR/${DOC_KEY}.md"

    # Get last updated date from frontmatter if it exists
    UPDATED="unknown"
    if [[ -f "$REF_FILE" ]]; then
      UPDATED=$(grep "^updated:" "$REF_FILE" 2>/dev/null | awk '{print $2}' || echo "unknown")
    fi

    # Extract files list (crude but avoids yq dependency)
    FILES_LINE=$(grep "^  ${DOC_KEY}:" "$CHANGES_FILE" || echo "")
    FILES_DISPLAY=""
    if [[ "$FILES_LINE" =~ files:[[:space:]]*\[([^\]]*)\] ]]; then
      FILES_DISPLAY="${BASH_REMATCH[1]}"
    fi

    if [[ "$COUNT" -ge "$THRESHOLD" ]]; then
      STALE_COUNT=$((STALE_COUNT + 1))
      echo "  ⚠  $DOC_KEY"
      echo "     Changes: $COUNT (above threshold of $THRESHOLD)"
      echo "     Last updated: $UPDATED"
      if [[ -n "$FILES_DISPLAY" ]]; then
        echo "     Changed files: $FILES_DISPLAY"
      fi
      echo "     Action: /write-architecture-doc references/${DOC_KEY}.md"
      echo ""
    elif [[ "$COUNT" -gt 0 ]]; then
      echo "  ~  $DOC_KEY"
      echo "     Changes: $COUNT (below threshold — monitoring)"
      echo "     Last updated: $UPDATED"
      echo ""
    else
      echo "  ✓  $DOC_KEY (no changes)"
    fi
  fi
done < "$CHANGES_FILE"

echo "════════════════════════════════════════"
echo "Summary: $STALE_COUNT/$TOTAL_COUNT docs need updating"
if [[ "$STALE_COUNT" -gt 0 ]]; then
  echo ""
  echo "Stale docs should be updated with /write-architecture-doc."
  echo "Each update resets the change counter for that doc."
fi
