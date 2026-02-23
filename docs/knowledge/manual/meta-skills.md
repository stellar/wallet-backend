---
description: Meta-skills for vault health and maintenance
type: reference
vault: docs/knowledge
---

# Meta-Skills

Meta-skills manage the health and evolution of the knowledge system itself.

## Health Check

```
/health
```

Runs 8 diagnostic categories:
1. Schema compliance — frontmatter validation
2. Orphan detection — entries with no incoming links
3. Link health — broken wiki-links
4. Context quality — description field length/quality
5. Three-space boundaries — ensures captures/ isn't a dumping ground
6. Processing throughput — captures that haven't been documented
7. Stale entries — current-status entries older than 90 days
8. Knowledge map coherence — subsystem maps linking to non-existent entries

## Stats

```
/stats
```

Shows:
- Total entries by type and subsystem
- Entry age distribution
- Coverage gaps (subsystems with few entries)
- Processing queue depth (unprocessed captures)

## Graph

```
/graph
```

Visualizes the knowledge graph — which entries link to which, orphans, highly-connected hubs.

## Tasks

```
/tasks
```

Shows `ops/tasks.md` — pending review tasks from code changes, improvement ideas, things to document.

## Retrospect

```
/retrospect
```

After completing a project or significant feature, captures:
- What worked well
- What was harder than expected
- Patterns to reuse
- Gotchas to document
