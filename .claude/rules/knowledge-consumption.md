---
paths:
  - "internal/**/*.go"
---

# Knowledge Consumption During Development

Before implementing changes, read the relevant subsystem knowledge map and reference doc.
Start at `docs/knowledge/entries/index.md` — it maps each subsystem to its docs.

## Consumption Depths

### Quick Scan — for small additions or bug fixes
1. Read the reference doc for the affected subsystem
2. Scan the **Gotchas** section of the knowledge map

### Deep Review — for new features, refactors, cross-subsystem work
1. Read the reference doc for EACH affected subsystem
2. Read the full knowledge map, focus on:
   - **Decisions** — understand why the current design exists before changing it
   - **Patterns** — follow established patterns unless there's a documented reason not to
   - **Tensions** — be aware of known unresolved conflicts
3. If your change touches two subsystems, read both maps and trace cross-links

## After Implementation

If your work reveals new insights, gotchas, or decisions, note them for `/document`.
The `track-code-changes.sh` hook will automatically create review tasks.
