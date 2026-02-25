#!/usr/bin/env bash
# auto-commit.sh — PostToolUse (Write) hook, async
# Auto-commits changes to docs/knowledge/

set -euo pipefail

FILE_PATH="${TOOL_INPUT_FILE_PATH:-}"

if [[ -z "$FILE_PATH" ]]; then
  exit 0
fi

# Only auto-commit docs/knowledge/ changes
if [[ "$FILE_PATH" != *"docs/knowledge/"* ]]; then
  exit 0
fi

# Check if we're in a git repo
if ! git rev-parse --git-dir &>/dev/null 2>&1; then
  exit 0
fi

# Check if there are changes to this file to commit
if git diff --quiet "$FILE_PATH" 2>/dev/null && ! git ls-files --others --exclude-standard "$FILE_PATH" | grep -q .; then
  exit 0
fi

# Also stage any other pending knowledge changes
git add docs/knowledge/ 2>/dev/null || true

# Build commit message from staged changes
CHANGED_FILES=$(git diff --cached --name-only 2>/dev/null | head -5)
FILE_COUNT=$(git diff --cached --name-only 2>/dev/null | wc -l | tr -d ' ')
STATS=$(git diff --cached --stat 2>/dev/null | tail -1)

if [[ "$FILE_COUNT" -eq 1 ]]; then
  FILENAME=$(echo "$CHANGED_FILES" | head -1)
  MSG="Auto: $FILENAME"
else
  MSG="Auto: $FILE_COUNT files"
fi

if [[ -n "$STATS" ]]; then
  MSG="$MSG | $STATS"
fi

git commit -m "$MSG" --no-verify 2>/dev/null || exit 0

exit 0
