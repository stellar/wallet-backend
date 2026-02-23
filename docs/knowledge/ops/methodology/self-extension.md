# Self-Extension

This system is not static. It evolves based on your actual experience using it. The principle: complexity arrives at pain points, not before. You don't add features because they seem useful — you add them because you've hit friction that proves they're needed.

---

## /remember-Driven Module Adoption

Every feature in this system is a module you can toggle. The question is never "should I use this?" but "am I feeling enough friction to justify this?"

**The pattern:**
1. Work with your current setup
2. Notice friction — something repeatedly takes too long, breaks, or gets forgotten
3. Use /remember to capture the friction signal (or let session capture detect it automatically from the transcript)
4. Identify which module addresses that friction
5. Activate it and adapt it to your domain
6. Monitor — did the friction decrease?

**Examples of friction that triggers adoption:**
- Can't find related entries → activate semantic search
- Keep creating duplicate entries → activate duplicate checking in processing
- Knowledge maps are too large to navigate → split into sub-knowledge maps
- Important entries have no connections → activate revisiting maintenance
- Same mistakes keep happening → activate validation hooks

**What NOT to do:** Activate everything at once. Each module adds cognitive overhead. A system with 15 active features that you only need 5 of is worse than one with 5 features you actually use.

---

## Methodology Folder

Your system maintains its own self-knowledge as linked entries in `ops/methodology/`. This is where the system records what it has learned about its own operation:

- **Derivation rationale** — Why each configuration choice was made (generated at init)
- **Friction captures** — Observations from /remember and automatic session mining
- **Configuration state** — Active features, thresholds, processing preferences

The methodology folder is referenced by meta-skills (/retrospect, /architect) when reasoning about system evolution. It is the substrate for self-awareness — without it, the system cannot explain why it works the way it does.

---

## Rule Zero: Methodology as Canonical Specification

Your methodology folder is more than a log — it is the canonical specification of how your system operates. This is Rule Zero: the meta-principle that governs all other rules.

**What this means in practice:**
- `ops/methodology/` is the source of truth for system behavior. When methodology entries say "check for duplicates before creating," the system should check for duplicates before creating.
- Changes to system behavior update methodology FIRST. /remember writes to `ops/methodology/` as its primary action. /retrospect reads methodology as the spec to compare against.
- Drift between methodology and actual behavior is automatically detected:
  - **Session start:** Lightweight staleness check — are methodology entries older than config changes?
  - **/next:** Coverage check — do active features have corresponding methodology entries?
  - **/retrospect:** Full assertion comparison — do methodology directives match actual system behavior?
- Drift observations feed back into the standard learning loop for triage and resolution.

Think of `ops/methodology/` as your system's constitution. Individual session decisions are statutes — they can change frequently. But the methodology specification is the foundational document that all decisions should align with. When they don't, that's drift, and drift is the signal for improvement.

---

## The Seed-Evolve-Reseed Lifecycle

Your knowledge system follows a natural lifecycle:

**1. Seed** — Start with a minimal system
The initial setup gives you: atomic entries, knowledge maps, wiki links, captures/, basic schema. This is enough to begin capturing and connecting.

**2. Evolve** — Adapt based on experience
As you use the system, friction reveals where it falls short. You add modules, adjust schemas, split knowledge maps, create new templates. The system becomes more yours.

**3. Reseed** — Reassess when accumulated drift warrants it
After significant evolution, the system may have accumulated complexity that no longer serves you. Reseed by asking: "If I started fresh today, knowing what I know, what would I keep?" This doesn't mean deleting everything — it means reconsidering which modules are earning their place.

---

## Observation Capture Protocol

Observations are the raw material for evolution. When you notice something about how the system is working (or not working), capture it immediately.

**Where:** `ops/observations/`

**What to capture:**
- Friction you experienced (what was hard, slow, or confusing)
- Surprises (what worked better or worse than expected)
- Process gaps (steps that should exist but don't)
- Methodology insights (why something works the way it does)

**Format:** Each observation is an atomic entry with a prose-sentence title:
```markdown
---
context: What happened and what it suggests
category: friction | surprise | process-gap | methodology
status: pending
observed: YYYY-MM-DD
---
# the observation as a sentence

What happened, why it matters, and what might change.
```

**Processing observations:**
When observations accumulate (roughly 5-10 pending), review them as a batch:
- Do patterns emerge? Multiple friction entries about the same area suggest a structural problem.
- Should any become entries? Some observations crystallize into genuine insights.
- Should any trigger system changes? Update ops/context.md, adjust templates, add or remove modules.
- Archive observations that have been processed or are no longer relevant.

---

## Complexity Curve Monitoring

**Signs of healthy complexity:**
- Every active module addresses a real friction point
- You can explain why each feature exists in one sentence
- New users of your system can understand the basics in under 10 minutes
- Maintenance doesn't take more than 10% of your work time

**Signs of unhealthy complexity:**
- Modules you activated but never use
- Templates with fields you never fill in
- Maintenance tasks that feel like busywork
- You spend more time organizing than thinking
- Fear of breaking something when you change anything

**The intervention:** When complexity feels unhealthy, run a module audit:
1. List every active feature
2. For each: when did you last use it? What friction does it address?
3. Deactivate anything that hasn't earned its place in the last month
4. Simplify schemas by removing fields you never query

Deactivating a module is not failure. It's the system correctly adapting to your actual needs versus your imagined needs.

---

## Configuration Changelog

Track what changed and why in `ops/changelog.md`:
```markdown
## YYYY-MM-DD: Brief description

**Changed:** What was modified
**Reason:** What friction or observation triggered this
**Outcome:** What improved (fill in after living with the change)
```

---

## Operational Learning Loop

Your system generates two types of signals during normal operation:

**Observations** — friction, surprises, process gaps, methodology insights. These go to `ops/observations/` as atomic entries. They tell you what's working and what isn't.

**Tensions** — contradictions between entries, conflicting methodology claims, implementation vs theory mismatches. These go to `ops/tensions/` as atomic entries. They tell you where your understanding is inconsistent.

Both signal types accumulate over time. The system monitors their count and suggests action when thresholds are crossed:
- **10+ pending observations** → suggest running /retrospect
- **5+ pending tensions** → suggest running /retrospect

The /retrospect command triages accumulated signals. For each pending entry, it decides one of four actions:
- **PROMOTE** to entries/ — the observation crystallized into a genuine insight worth keeping as a permanent entry
- **IMPLEMENT** as system change — the observation points to a concrete improvement in ops/context.md, templates, or workflows
- **ARCHIVE** — the observation was session-specific or no longer relevant
- **KEEP PENDING** — not enough evidence yet to decide; let it accumulate with others

---

## The Self-Building Loop

Your knowledge system doesn't just maintain itself — it actively grows:

1. **/learn [topic]** — Research a topic using available sources. Results are filed to captures/ with provenance metadata so you can trace where every claim came from.
2. **/document** — Extract atomic entries from captures/ material. Each source gets scanned through the wallet-backend engineering lens.
3. **/connect** — Find connections between new entries and existing ones. Update knowledge maps. Add bidirectional links where relationships are genuine.
4. **Compound** — The graph grows. New connections make existing entries more valuable by creating new traversal paths.
5. **Repeat** — Each cycle makes the system more capable. More entries means more connections. More connections means better retrieval.
