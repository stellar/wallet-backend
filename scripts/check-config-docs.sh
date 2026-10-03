#!/usr/bin/env bash
# Checks that docs/configuration.md documents every CLI flag and no flag that does not exist.
# Flags come from `--help` of each subcommand. The doc may name a flag as `--foo-bar` or as
# its env var `FOO_BAR` (both in backticks, optionally followed by =value). Env tokens need an
# underscore so enum values like `INFO` are not read as flags; wildcards like `--db-*` are skipped.
# shellcheck disable=SC2016,SC2001 # backticks are literal; sed prefixes multi-line lists
set -euo pipefail

cd "$(dirname "$0")/.."

doc="${CONFIG_DOC:-docs/configuration.md}"
if [[ ! -f "$doc" ]]; then
  echo "ERROR: $doc not found" >&2
  exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

go build -o "$tmp/wallet-backend" .

commands=(
  "serve"
  "ingest"
  "migrate up"
  "protocol-setup"
  "protocol-migrate history"
  "protocol-migrate current-state"
)

for cmd in "${commands[@]}"; do
  # shellcheck disable=SC2086 # word splitting turns "migrate up" into two args
  "$tmp/wallet-backend" $cmd --help |
    sed -nE 's/^[[:space:]]+(-[a-zA-Z], )?--([a-z0-9-]+).*/\2/p'
done | grep -vx 'help' | sort -u >"$tmp/cli"

{
  grep -oE '`--[a-z0-9]+(-[a-z0-9]+)*[`= ]' "$doc" | sed -E 's/^`--//; s/.$//' || true
  grep -oE '`[A-Z][A-Z0-9]*(_[A-Z0-9]+)+(=[^`]*)?`' "$doc" |
    sed -E 's/^`//; s/[=`].*$//' | tr 'A-Z_' 'a-z-' || true
} | sort -u >"$tmp/doc"

missing="$(comm -23 "$tmp/cli" "$tmp/doc")"
unknown="$(comm -13 "$tmp/cli" "$tmp/doc")"

if [[ -n "$missing" || -n "$unknown" ]]; then
  if [[ -n "$missing" ]]; then
    echo "Flags missing from $doc:"
    sed 's/^/  --/' <<<"$missing"
  fi
  if [[ -n "$unknown" ]]; then
    echo "Flags in $doc that no command has:"
    sed 's/^/  --/' <<<"$unknown"
  fi
  exit 1
fi

echo "OK"
