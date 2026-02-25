# Maintenance Methodology

A knowledge graph degrades without maintenance. Entries written last month don't know about entries written today. Links break when titles change. Knowledge maps grow stale as topics evolve. Maintenance is not optional — it's what keeps the system useful.

---

## Health Check Categories

Run these checks when conditions warrant — orphans detected, links broken, schema violations accumulated:

**1. Orphan Detection**
Entries with no incoming links are invisible to traversal. Find them:
```bash
# Find entries not linked from anywhere
rg -l '.' docs/knowledge/entries/*.md | while read f; do
  title=$(basename "$f" .md)
  rg -q "\[\[$title\]\]" docs/knowledge/entries/ || echo "Orphan: $f"
done
```
Every orphan is either a gap (needs connections) or stale (needs archiving).

**2. Dangling Links**
Wiki links pointing to non-existent entries create confusion:
```bash
# Find [[links]] to files that don't exist
rg -o '\[\[([^\]]+)\]\]' docs/knowledge/entries/ -r '$1' --no-filename | sort -u | while read title; do
  find . -name "$title.md" -not -path "./.git/*" | grep -q . || echo "Dangling: [[$title]]"
done
```
Either create the missing entry or fix the link.

**3. Schema Validation**
Check that entries have required YAML fields:
```bash
rg -L '^context:' docs/knowledge/entries/*.md    # missing context fields
```
Missing context means the entry can't be filtered during search.

**4. Knowledge Map Coherence**
Knowledge maps should accurately reflect the entries they organize:
- Do all listed entries still exist?
- Are there entries on this topic NOT listed in the knowledge map?
- Has the topic grown large enough to warrant splitting?

**5. Stale Content**
Entries that haven't been touched in a long time may contain outdated claims. Check modification dates and review oldest entries first.

---

## Revisiting — The Backward Pass

New entries create connections going forward. But older entries don't automatically know about newer ones. Revisiting is the practice of going back to old entries and asking: **"If I wrote this today, what would be different?"**

Entries are living documents, not finished artifacts. An entry written last month was written with last month's understanding. Since then: new entries exist, understanding deepened, the claim might need sharpening, what was one idea might now be three.

**What revisiting can do:**

| Action | When |
|--------|------|
| Add connections | Newer entries exist that should link here |
| Rewrite content | Understanding evolved, prose should reflect it |
| Sharpen the claim | Title is too vague to be useful |
| Split the entry | Multiple claims bundled together |
| Challenge the claim | New evidence contradicts the original |

**When to revisit:**
- After creating a batch of new entries — check what older entries should link to them
- When a health check flags sparse entries with few connections
- When entries have not been touched since N new entries were added to the graph
- When an entry feels disconnected from the rest of the graph

Without revisiting, the vault becomes a graveyard of outdated thinking that happens to be organized. With revisiting, every entry stays current.

**The complete maintenance cycle:**
```
CREATE -> CONNECT FORWARD (/connect) -> REVISIT -> QUESTION/REWRITE/SPLIT (/maintain) -> EVOLVE
```

---

## Condition-Based Maintenance

Maintenance triggers are condition-based, not time-based. Time-based triggers assume uniform activity — a vault that scales fast would overwhelm a monthly check, while a vault used rarely would run empty checks on schedule. Condition-based triggers respond to actual state, firing exactly when the system needs attention.

| Condition | Threshold | Action When True |
|-----------|-----------|-----------------|
| Orphan entries | Any detected | Surface for connection-finding |
| Dangling links | Any detected | Surface for resolution |
| Knowledge map size | >40 entries | Suggest sub-knowledge map split |
| Pending observations | >=10 | Suggest /retrospect |
| Pending tensions | >=5 | Suggest /retrospect |
| Inbox pressure | Items older than 3 days | Suggest processing |
| Stale pipeline batch | >2 sessions without progress | Surface as blocked |
| Schema violations | Any detected | Surface for correction |

These conditions are evaluated by `/next` via queue reconciliation. When a condition fires, it materializes as a `type: "maintenance"` entry in the queue — not a calendar reminder.

---

## Session Maintenance Checklist

Before ending a work session:
- [ ] New entries are linked from at least one knowledge map
- [ ] Wiki links in new entries point to real files
- [ ] Context fields add information beyond the title
- [ ] Changes are committed

---

## Unified Queue Reconciliation

Maintenance work lives alongside pipeline work in the same queue. `/next` evaluates conditions and materializes maintenance tasks directly in the queue.

The reconciliation pattern:
1. **Declare conditions** — the system defines what "healthy" looks like via `maintenance_conditions` in the queue
2. **Measure actual state** — `/next` compares reality against each condition on every invocation
3. **Auto-create tasks** — when a condition is violated, a `type: "maintenance"` entry appears in the queue
4. **Auto-close tasks** — when the condition is satisfied again, the entry is marked done

This is idempotent: running `/next` any number of times produces the same queue state (no duplicates). Each `condition_key` has at most one pending maintenance task.

The key insight: you don't manage task status manually. Fix the underlying problem and the task goes away on its own.

---

## Invariant-Based Task Creation

The reconciliation checks invariants that together define a healthy system:

| Invariant | What It Checks |
|-----------|---------------|
| Inbox pressure (per subdir) | Are captures/ subdirectories accumulating unprocessed material? |
| Orphan entries | Are there entries with no incoming links? |
| Dangling links | Do wiki links point to non-existent entries? |
| Observation accumulation | Have pending observations exceeded the threshold (10+)? |
| Tension accumulation | Have pending tensions exceeded the threshold (5+)? |
| Knowledge map size | Has any knowledge map grown beyond its healthy range? |
| Stale batches | Are there processing batches that have been sitting unfinished? |
| Infrastructure ideas | Are there improvement ideas waiting for review? |
| Pipeline pressure | Is the processing queue backing up? |
| Schema compliance | Do all entries pass schema validation? |
| Stale reference docs | Have >= 5 source files changed in a subsystem since the corresponding reference doc was last updated? Tracked in `ops/reference-doc-changes.yaml`, incremented by the `track-code-changes.sh` hook, reset by `/write-architecture-doc` Phase 6. Consequence speed: multi-session (agents load increasingly inaccurate architecture context). |

Each invariant is self-healing: fix the underlying issue (process the captures, connect the orphan, resolve the tension) and the task disappears at next reconciliation. No manual status updates needed.

---

## Consequence-Speed Priority

Maintenance tasks are prioritized by how fast their consequences compound, not by manual importance labels:

| Consequence Speed | Priority | Examples | Why This Priority |
|-------------------|----------|----------|-------------------|
| `session` | Highest | Orphan entries, dangling links, inbox pressure | These degrade your work quality right now |
| `multi_session` | Medium | Pipeline batch completion, stale batches | These compound over days — unfinished batches block downstream work |
| `slow` | Lower | Knowledge map oversizing, retrospect thresholds, infrastructure ideas | These compound over weeks — annoying but not blocking |

---

## The Maintenance Mindset

Maintenance is not cleanup — it's cultivation. Each pass through old entries is an opportunity to deepen the graph. Revisiting discovers connections that weren't visible when the entries were first written. Health checks reveal structural gaps that point toward missing insights.

The graph doesn't just get maintained. It gets better.
