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

# Stage the specific file
git add "$FILE_PATH" 2>/dev/null || exit 0

# Commit with a standard message
BASENAME=$(basename "$FILE_PATH")
git commit -m "knowledge: update $BASENAME" --no-verify 2>/dev/null || exit 0

exit 0
