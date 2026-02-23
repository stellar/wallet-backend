---
description: Common problems and solutions for the knowledge system
type: reference
vault: docs/knowledge
---

# Troubleshooting

## "My entry failed schema validation"

The `validate-note.sh` hook checks entries written to `entries/` and `references/`. Common issues:

| Error | Fix |
|-------|-----|
| Missing `description` | Add a one-line description in frontmatter |
| Missing `type` | Add `type: decision` (or insight/pattern/gotcha/reference) |
| Invalid `subsystem` | Use one of: ingestion, graphql, data-layer, signing, services, auth |
| Missing `status` | Add `status: current` |
| Missing `areas` | Add `areas: [at-least-one-tag]` |

## "Session-orient.sh is failing"

Check that `docs/knowledge/` exists and `docs/knowledge/.arscontexta` is present. If the vault is corrupted, run `/health` to diagnose.

## "Reference doc is out of date after code change"

The `track-code-changes.sh` hook should have created a task in `ops/tasks.md`. If it didn't, the changed file may not match any subsystem pattern. Add the pattern to `track-code-changes.sh`.

## "I have too many unprocessed captures"

Run `/pipeline` to batch-process the captures queue. Or run `/next` to get the highest-priority item to work on.

## "Knowledge map has broken links"

Run `/ralph` to detect and fix broken links. Or run `/health` in full mode to get a list of broken links.

## "An entry is superseded but still appearing"

Mark it: add `status: superseded` and `superseded_by: [[new-entry-name]]` to the frontmatter. The `/graph` skill will show the supersession chain.
