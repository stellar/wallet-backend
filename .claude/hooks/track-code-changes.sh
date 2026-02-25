#!/usr/bin/env bash
# track-code-changes.sh — PostToolUse (Edit) hook, async
# Detects code edits and creates review tasks in docs/knowledge/ops/tasks/

set -euo pipefail

FILE_PATH="${TOOL_INPUT_FILE_PATH:-}"

if [[ -z "$FILE_PATH" ]]; then
  exit 0
fi

# Only track Go source files
if [[ "$FILE_PATH" != *.go ]]; then
  exit 0
fi

# Skip test files
if [[ "$FILE_PATH" == *_test.go ]]; then
  exit 0
fi

TASKS_DIR="docs/knowledge/ops/tasks"
mkdir -p "$TASKS_DIR"

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
BASENAME=$(basename "$FILE_PATH" .go)
TASK_FILE="$TASKS_DIR/review-${BASENAME}-$(date -u +%Y%m%d%H%M%S).md"

cat > "$TASK_FILE" << EOF
---
type: review-task
file: $FILE_PATH
created: $TIMESTAMP
status: pending
---

# Review: $BASENAME

Code change detected in \`$FILE_PATH\`.

## Task

Review this change for knowledge-worthy insights:
- Architecture decisions made
- Patterns introduced or changed
- Gotchas discovered
- Performance considerations

If insights found, run \`/document\` on this file.
EOF

# --- Reference doc change accumulation ---
# Match FILE_PATH against prefix→doc_key mappings (longest-prefix-first ordering
# in reference-doc-dirs.txt ensures specificity: internal/serve/auth before internal/serve)

DIRS_FILE="docs/knowledge/ops/reference-doc-dirs.txt"
CHANGES_FILE="docs/knowledge/ops/reference-doc-changes.yaml"

if [[ -f "$DIRS_FILE" ]] && [[ -f "$CHANGES_FILE" ]]; then
  MATCHED_KEY=""
  while IFS=: read -r PREFIX DOC_KEY; do
    # Skip comment lines
    [[ "$PREFIX" =~ ^[[:space:]]*# ]] && continue
    [[ -z "$PREFIX" ]] && continue
    if [[ "$FILE_PATH" == "$PREFIX"/* ]] || [[ "$FILE_PATH" == "$PREFIX" ]]; then
      MATCHED_KEY="$DOC_KEY"
      break
    fi
  done < "$DIRS_FILE"

  if [[ -n "$MATCHED_KEY" ]]; then
    # Increment count using sed (no yq dependency)
    # Pattern: "  key: { count: N, files: [...]  }"
    CURRENT_COUNT=$(grep "^  ${MATCHED_KEY}:" "$CHANGES_FILE" | grep -o 'count: [0-9]*' | grep -o '[0-9]*' || echo "0")
    NEW_COUNT=$((CURRENT_COUNT + 1))

    # Update the count
    sed -i.bak "s/^  ${MATCHED_KEY}: { count: ${CURRENT_COUNT},/  ${MATCHED_KEY}: { count: ${NEW_COUNT},/" "$CHANGES_FILE"

    # Append file to the files list (cap at 20 entries to avoid unbounded growth)
    CURRENT_LINE=$(grep "^  ${MATCHED_KEY}:" "$CHANGES_FILE")
    if [[ "$CURRENT_LINE" =~ files:[[:space:]]*\[\] ]]; then
      # Empty list — replace [] with [file]
      sed -i.bak "s|^  ${MATCHED_KEY}: { count: ${NEW_COUNT}, files: \[\] }|  ${MATCHED_KEY}: { count: ${NEW_COUNT}, files: [${FILE_PATH}] }|" "$CHANGES_FILE"
    else
      # Non-empty list — extract existing files, check count, append if under cap
      FILE_COUNT=$(echo "$CURRENT_LINE" | grep -o ',' | wc -l | tr -d ' ')
      if [[ "$FILE_COUNT" -lt 20 ]]; then
        # Append before closing ]
        sed -i.bak "s|^  ${MATCHED_KEY}: { count: ${NEW_COUNT}, files: \[\(.*\)\] }|  ${MATCHED_KEY}: { count: ${NEW_COUNT}, files: [\1, ${FILE_PATH}] }|" "$CHANGES_FILE"
      fi
    fi

    # Clean up backup file
    rm -f "${CHANGES_FILE}.bak"

    # Add a note to the review task about the affected reference doc
    cat >> "$TASK_FILE" << REFEOF

## Reference Doc Impact

This file is tracked by \`references/${MATCHED_KEY}.md\`.
If this change affects subsystem architecture, run \`/write-architecture-doc references/${MATCHED_KEY}.md\`.
REFEOF
  fi
fi

exit 0
