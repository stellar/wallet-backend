# Task Management

Processing multiple entries through a multi-phase pipeline requires tracking. Without it, entries get stuck mid-pipeline, phases get skipped, and you lose visibility into what has been done and what remains.

---

## Task Queue

The task queue tracks every entry being processed through the pipeline. It lives at `ops/queue/queue.json`:

```json
{
  "tasks": [
    {
      "id": "source-name-001",
      "type": "claim",
      "status": "pending",
      "target": "entry title here",
      "batch": "source-name",
      "created": "2026-02-13T10:00:00Z",
      "current_phase": "connect",
      "completed_phases": ["document", "create"]
    }
  ]
}
```

**Phase tracking:** Each entry has ONE queue entry. Phase progression is tracked via `current_phase` (the next phase to run) and `completed_phases` (what is already done). After each phase completes: append to `completed_phases`, advance `current_phase`. When the last phase completes: set `status` to `"done"`.

**Task types:**

| Type | Purpose | Phase Sequence |
|------|---------|---------------|
| extract | Extract entries from source | (single phase) |
| claim | Process a new entry through all phases | create → connect → maintain → verify |
| enrichment | Enrich an existing entry then process | enrich → connect → maintain → verify |

**Recovery:** If you crash mid-phase, the queue still shows `current_phase` at the failed phase. Re-running the pipeline picks it up automatically — no manual intervention needed.

---

## Per-Entry Task Files

Each extracted entry gets its own task file that accumulates notes across all phases. The task file is the shared state between phases — it is how one phase communicates what it found to the next phase.

```markdown
# Entry 001: {title}

## Document Notes
{Extraction rationale, duplicate judgment}

## Create
{Entry creation details}

## Connect
{Connections found, knowledge maps updated}

## Maintain
{Older entries updated with backward connections}

## Verify
{Context quality result, schema result, health result}
```

**Why task files matter:** Each phase reads the task file in fresh context. Downstream phases see what upstream phases discovered. Without this, context would be lost between sessions. The task file IS the memory of the pipeline.

**Batch archival:** When all entries from a source reach `"done"`, the batch is archivable. Archival moves task files to `ops/queue/archive/{date}-{source}/`, generates a summary, and cleans the queue.

---

## Maintenance Queue (Condition-Based Tasks)

Maintenance work lives alongside pipeline work in the same queue. Conditions materialize as `type: "maintenance"` queue entries with priority based on consequence speed. `/next` evaluates conditions on each invocation: fired conditions create queue entries, satisfied conditions auto-close them.

This is idempotent: running `/next` any number of times produces the same queue state (no duplicates). Each `condition_key` has at most one pending maintenance task.

For the full condition table and consequence-speed priority, see [[maintenance-methodology#Condition-Based Maintenance]].

---

## Orchestrated Processing (Fresh Context Per Phase)

The pipeline's quality depends on each phase getting your best attention. Your context degrades as conversation grows. The first ~40% of your context window is the "smart zone" — sharp, capable, good decisions. Beyond that, context rot sets in.

**The orchestration pattern:**

```
Orchestrator reads queue -> picks next task -> spawns worker for one phase
  Worker: fresh context, reads task file, executes phase, writes results to task file
  Worker returns -> Orchestrator reads results -> advances queue -> spawns next phase
```

**Why fresh context matters:**
- Document/extract needs full attention on the source material
- Connect needs full attention on the existing knowledge graph
- Maintain/revisit needs full attention on older entries
- Verify needs neutral perspective, unbiased by creation

If all four phases run in one session, the verify phase runs on degraded attention — you have already decided this entry is good during create, and confirmation bias sets in. Fresh context prevents this.

**Handoff through files, not context:**
- Each phase writes its findings to the task file
- The next phase reads the task file in fresh context
- State transfers through persistent files, not accumulated conversation
- This makes crashes recoverable and processing auditable

**Processing modes:**

| Mode | Behavior | When to Use |
|------|----------|-------------|
| Interactive | You invoke each phase manually, review between | Learning the system, important sources |
| Orchestrated | Automated phase-by-phase with fresh context | Batch processing, high volume |
| Compressed | Phases run sequentially in same session (quality trade-off) | Quick processing, minor sources |

Default: Interactive mode for new users, Orchestrated for experienced users. Configurable via `ops/config.yaml`:
```yaml
pipeline:
  processing_mode: interactive  # interactive | orchestrated | compressed
```

---

## Processing Depth Configuration

Not every source deserves the same attention. A critical research paper warrants fresh context per phase and maximum quality gates. A quick note from a conversation can be processed in a single pass.

**Three depth levels** (configurable via `ops/config.yaml`):

| Level | Behavior | Context Strategy | Use When |
|-------|----------|-----------------|----------|
| Deep | Full pipeline, fresh context per phase, maximum quality gates | Spawn subagent per phase | Important sources, research, initial vault building |
| Standard | Full pipeline, balanced attention, inline execution | Sequential phases in current session | Regular processing, moderate volume (default) |
| Quick | Compressed pipeline, combine connect+verify phases | Fast single-pass | High volume catch-up, minor sources |

```yaml
# ops/config.yaml
processing:
  depth: standard  # deep | standard | quick
```

---

## Pipeline Chaining

Skills need to chain their outputs. Without chaining, the pipeline is documentation — you complete one phase but forget to run the next.

| Mode | Behavior | Who Controls |
|------|----------|-------------|
| Manual | Skill outputs "Next: /skill [target]" — you decide whether to run it | You |
| Suggested | Skill outputs next step AND adds to task queue — you can skip | You + system |
| Automatic | Skill completes → next phase runs immediately via orchestration | System |

Default: Suggested mode. Automatic mode activates for batch/orchestrated processing.

```yaml
# ops/config.yaml
pipeline:
  chaining: suggested  # manual | suggested | automatic
```

**The chaining model:**

```
/learn [topic]
    -> files to captures/ with provenance
    -> [CHAIN] queue for document
/document [captures-file]
    -> extracts entries and enrichments to task queue
    -> [CHAIN] queues all extracted entries for create
create [entry]
    -> writes entry to entries/
    -> [CHAIN] queues entry for connect
/connect [entry]
    -> finds connections, updates knowledge maps
    -> [CHAIN] queues entry for maintain
/maintain [entry]
    -> updates old entries with new connections
    -> [CHAIN] queues entry for verify
/verify [entry]
    -> tests context, schema, links
    -> [END] marks task complete
```
