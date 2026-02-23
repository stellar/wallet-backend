# Pipeline Methodology

**Depth over breadth. Quality over speed. Tokens are free.**

Every piece of content follows the same path: capture → document → connect → verify. Each phase has a distinct purpose. Mixing them degrades both.

---

## The Four-Phase Skeleton

### Phase 1: Capture

Zero friction. Everything enters through `captures/`. Speed of capture beats precision of filing. Accept whatever arrives — no structuring at capture time.

Capture and document are temporally separated because context is freshest at capture but quality requires focused attention. If you try to capture AND document simultaneously, you will either capture slowly (losing insights to friction) or document poorly (rushing connections).

Capture everything. Document later.

### Phase 2: Document

This is where value is created. Raw content becomes structured entries through active transformation.

Read the source material through the mission lens: "Does this serve wallet-backend engineering?" Every extractable insight gets pulled out:

| Category | What to Find | Output |
|----------|--------------|--------|
| Core claims | Direct assertions about the domain | entry |
| Patterns | Recurring structures across sources | entry |
| Tensions | Contradictions or conflicts | Tension entry |
| Enrichments | Content that improves existing entries | Enrichment task |
| Anti-patterns | What breaks, what to avoid | Problem entry |

**The selectivity gate:** Not everything extracts. Judge: does this add genuine insight, or is it noise? When in doubt, extract — it is easier to merge duplicates than recover missed insights.

**Quality bar for extracted entries:**
- Title works as prose when linked: `since [[entry title]]` reads naturally
- Context adds information beyond the title
- Claim is specific enough to disagree with
- Reasoning is visible — shows the path to the conclusion

### Phase 3: Connect

After documenting creates new entries, connection finding integrates them into the existing knowledge graph.

**Forward connections:** What existing entries relate to this new one? Search semantically (not just keyword) — connections often exist between entries that use different vocabulary for the same concept.

**Backward connections:** What older entries need updating now that this new one exists? An entry written last week was written with last week's understanding. If today's entry extends, challenges, or provides evidence for the older one, update the older one.

**Knowledge map updates:** Every new entry belongs in at least one knowledge map. Add it with a context phrase explaining WHY it belongs — bare links without context are useless for navigation.

**Connection quality standard:** Not just "related to" but "extends X by adding Y" or "contradicts X because Z." Every connection must articulate the relationship.

For the backward pass philosophy and when revisiting is warranted, see [[maintenance-methodology#Revisiting — The Backward Pass]].

### Phase 4: Verify

Three checks in one phase:

1. **Context quality (cold-read test)** — Read ONLY the title and context. Without reading the body, predict what the entry contains. Then read the body. If your prediction missed major content, the context needs improvement.

2. **Schema compliance** — All required fields present, enum values valid, topic links exist, no unknown fields. The template `_schema` block defines what is valid.

3. **Health check** — No broken wiki links, no orphaned entries, link density within healthy range (2+ outgoing links per entry).

**Failure handling:** Context quality failures → rewrite context immediately. Schema failures → fix immediately. Link failures → log for the connect phase next pass.

---

## Inbox Processing

Everything enters through `captures/`. Do not think about structure at capture time — just get it in.

**What goes to captures/:**
- URLs with a brief note about why they matter
- Quick ideas and observations
- Sources (PDFs, articles, research results)
- Voice capture transcripts
- Anything where destination is unclear

**Processing captures/ items:** Read the item → extract insights → create atomic entries in `entries/` → link to knowledge maps → move or delete the captures/ item.

**The core principle:** Capture needs to be FAST (zero friction). Processing needs to be SLOW (careful extraction, quality connections). Separating these activities is what makes both work.

---

## Processing Principles

- **Fresh context per phase** — Each phase benefits from focused attention. Critical work should happen when context is fresh.
- **Quality over speed** — One well-connected entry is worth more than ten orphaned ones. The graph compounds quality, not quantity.
- **The generation effect** — Moving information is not processing. You must TRANSFORM it: generate context, find connections, create synthesis. Passive transfer does not create understanding.
- **Skills encode methodology** — If a skill exists for a processing step, use it. Do not manually replicate the workflow. Skills contain quality gates that manual execution bypasses.

---

## Quality Gates Summary

Every phase has specific gates. Failing a gate does not block progress — it triggers correction.

| Phase | Gate | Failure Action |
|-------|------|---------------|
| Document | Selectivity — is this worth extracting? | Skip with logged reason |
| Document | Composability — does the title work as prose? | Rewrite title |
| Document | Context adds new info beyond title? | Rewrite context |
| Document | Duplicate check — semantic search run? | Run search, merge if duplicate |
| Connect | Genuine relationship — can you say WHY? | Do not force the connection |
| Connect | Knowledge map updated | Add entry to relevant knowledge maps |
| Connect | Backward pass — older entries updated? | Update or log for maintenance |
| Verify | Context predicts content (cold-read test) | Improve context |
| Verify | Schema valid | Fix schema violations |
| Verify | No broken links | Fix or remove broken links |
| Verify | Entry in at least one knowledge map | Add to relevant knowledge map |

---

## Skill Invocation Rules

If a skill exists for a task, use the skill. Do not manually replicate the workflow. Skills encode the methodology — manual execution bypasses quality gates.

| Trigger | Required Skill |
|---------|----------------|
| New content to document | /document |
| New entries need connections | /connect |
| Old entries may need updating | /revisit |
| Quality verification needed | /verify |
| System health check | /health |
| User asks to find connections | /connect (not manual grep) |
| System feels disorganized | /health (systematic checks, not ad-hoc) |

**The enforcement principle:** If a skill exists for a task, use the skill. Do not improvise the workflow manually. Manual execution loses the quality gates.

---

## Research Provenance

Every file in `captures/` from a research tool (web search, deep research, API import) MUST include provenance metadata in its YAML frontmatter. Claims without provenance are untraceable.

**Standard provenance fields:**

```yaml
source_type: [research | web-search | manual | voice | channel | import]
research_prompt: "the query or directive that generated this content"
research_server: "exa | google | brave | manual"
research_model: "exa-research-pro | exa-research-fast | n/a"
generated: "YYYY-MM-DDTHH:MM:SSZ"
```

The `research_prompt` field is the most critical — it captures the intellectual context that shaped what was returned. Knowing "I searched for X because I was exploring Y" is part of the knowledge graph.

**Provenance chain:** research query → captures/ file (YAML with prompt) → document → entries. Each entry's Source footer links back to the captures/ file.
