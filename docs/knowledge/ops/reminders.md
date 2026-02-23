---
description: Periodic reminders and maintenance prompts for the knowledge system
type: reference
vault: docs/knowledge
---

# Reminders

_Format: one reminder per line, with suggested frequency_

## Periodic

- [ ] **Weekly**: Run `/health quick` to check schema compliance and orphan entries
- [ ] **Monthly**: Run `ops/queries/stale-decisions.sh` to find aging entries
- [ ] **Monthly**: Run `ops/queries/todo-references.sh` to track reference doc completion
- [ ] **Quarterly**: Run `/health full` for comprehensive vault diagnostics

## Condition-based (automated via hooks)

- Triggered automatically when code changes touch a known subsystem directory
- Review tasks appear in `ops/tasks.md` — check there at session start

## One-time Setup Tasks

- [ ] Document the TimescaleDB choice (why not vanilla PostgreSQL)
- [ ] Document the channel account design decision
- [ ] Document the schema-first GraphQL decision
- [ ] Complete TODO sections in references/signing-and-channels.md
- [ ] Complete TODO sections in references/services.md
- [ ] Complete TODO sections in references/authentication.md
- [ ] Complete TODO sections in references/overview.md
