#!/usr/bin/env bash
# validate-note.sh — PostToolUse (Write) hook
# Validates YAML frontmatter on writes to docs/knowledge/entries/

set -euo pipefail

# Only run for writes to the entries directory
FILE_PATH="${TOOL_INPUT_FILE_PATH:-}"

if [[ -z "$FILE_PATH" ]]; then
  exit 0
fi

# Only validate files in entries/
if [[ "$FILE_PATH" != *"docs/knowledge/entries/"* ]]; then
  exit 0
fi

# Only validate .md files
if [[ "$FILE_PATH" != *.md ]]; then
  exit 0
fi

# Check for required frontmatter fields
if [[ -f "$FILE_PATH" ]]; then
  # Check for YAML frontmatter delimiters
  if ! head -1 "$FILE_PATH" | grep -q "^---$"; then
    echo "WARNING: $FILE_PATH is missing YAML frontmatter delimiters (---)"
  fi

  # Check for context (description) field
  if ! grep -q "^context:" "$FILE_PATH" && ! grep -q "^description:" "$FILE_PATH"; then
    echo "WARNING: $FILE_PATH is missing 'context' field in frontmatter"
  fi

  # Check for type field
  if ! grep -q "^type:" "$FILE_PATH"; then
    echo "WARNING: $FILE_PATH is missing 'type' field in frontmatter"
  fi
fi

exit 0
