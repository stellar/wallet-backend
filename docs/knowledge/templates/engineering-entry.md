---
description: <one-line summary — what is this entry about?>
type: decision  # decision | insight | pattern | gotcha | reference
status: current  # current | superseded | experimental
confidence: likely  # proven | likely | experimental
subsystem: ingestion  # ingestion | graphql | data-layer | signing | services | auth
areas: [<tag1>, <tag2>]
# superseded_by: [[other-entry]]  # uncomment if this entry is superseded
---

_schema:
  entity_type: engineering-entry
  applies_to: "entries/*.md"
  required: [description, type, status, subsystem, areas]
  optional: [confidence, superseded_by]
  enums:
    type: [decision, insight, pattern, gotcha, reference]
    status: [current, superseded, experimental]
    confidence: [proven, likely, experimental]
    subsystem: [ingestion, graphql, data-layer, signing, services, auth]

# Entry Title

## Context

<!-- What problem or situation prompted this entry? What was the state of things before? -->

## Detail

<!-- The main content. For decisions: what was chosen and why. For insights: the non-obvious understanding.
     For patterns: how to apply it. For gotchas: what to watch out for. -->

## Implications

<!-- What does this mean for the codebase? What should engineers do or avoid? -->

## Source

<!-- Optional: what file, PR, or event generated this knowledge? -->
<!-- Source: internal/services/ingest.go:ProcessLedger -->

## Related

<!-- [[related-entry-1]] -->
<!-- [[references/ingestion-pipeline]] -->
