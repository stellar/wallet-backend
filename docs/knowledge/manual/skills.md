---
description: Available skills for the wallet-backend knowledge system
type: reference
vault: docs/knowledge
---

# Skills Reference

All knowledge system skills are invoked with `/skillname`.

## Core Processing Skills

| Skill | Trigger | Purpose |
|-------|---------|---------|
| `/document` | After debugging, after a decision | Turn raw knowledge into structured entries |
| `/connect` | After documenting | Link entries, update knowledge maps |
| `/revisit` | When entries may be stale | Review and update aging entries |
| `/verify` | Before using an entry for major work | Confirm accuracy against source code |

## Capture & Discovery

| Skill | Trigger | Purpose |
|-------|---------|---------|
| `/seed` | New repo, empty vault | Seed initial entries from existing docs |
| `/learn` | After reading source code | Extract knowledge patterns from code |
| `/remember` | Want to persist something specific | Save a targeted insight |

## Navigation & Query

| Skill | Trigger | Purpose |
|-------|---------|---------|
| `/graph` | Exploring knowledge structure | Visualize entry relationships |
| `/next` | Not sure what to work on | Get the next recommended action |
| `/pipeline` | Process multiple captures | Batch-process captures queue |

## Maintenance

| Skill | Trigger | Purpose |
|-------|---------|---------|
| `/validate` | Before committing | Check schema compliance |
| `/health` | Periodic maintenance | Full vault health diagnostics |
| `/stats` | Curiosity about vault state | Entry counts, coverage, gaps |
| `/tasks` | See pending work | View and manage task stack |
| `/retrospect` | After completing a project | Capture learnings and patterns |
| `/refactor` | Structural drift | Reorganize without losing content |
| `/ralph` | Major changes | Review all links after a move |
