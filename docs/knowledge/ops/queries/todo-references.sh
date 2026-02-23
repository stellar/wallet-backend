#!/usr/bin/env bash
# todo-references.sh — Reference docs with remaining TODO sections
set -euo pipefail

VAULT="docs/knowledge"

echo "Reference docs with TODO sections"
echo ""

find "$VAULT/references" -name "*.md" | while read -r file; do
  TODO_COUNT=$(grep -c "<!-- TODO:" "$file" 2>/dev/null || echo 0)
  if [[ "$TODO_COUNT" -gt 0 ]]; then
    echo "  $TODO_COUNT TODO(s): $file"
    grep "<!-- TODO:" "$file" | head -3 | sed 's/^/    /'
  fi
done
