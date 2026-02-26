---
description: Machine-readable manifest for runtime skill vocabulary and configuration
type: reference
vault: docs/knowledge
---

# Derivation Manifest

Generated: 2026-02-23
Plugin version: arscontexta 0.8.0
Repo: wallet-backend

## Vault Identity

```yaml
vault_root: docs/knowledge
vault_marker: docs/knowledge/.arscontexta
repo: wallet-backend
```

## Vocabulary Mapping

```yaml
note: entry
notes_dir: entries
inbox: captures
inbox_dir: captures
moc: knowledge map
mocs_in: entries
reduce_skill: document
reflect_skill: connect
reweave_skill: revisit
rethink_skill: retrospect
```

## Schema

```yaml
entry_required: [description, type, status, subsystem, areas]
entry_optional: [confidence, superseded_by]
entry_types: [decision, insight, pattern, gotcha, reference, question, fact, tension]
entry_statuses: [current, active, superseded, experimental, open]
entry_confidences: [proven, likely, experimental]
subsystems: [ingestion, graphql, data-layer, signing, services, auth]
```

## Navigation

```yaml
hub: entries/index.md
subsystem_maps:
  ingestion: entries/ingestion.md
  graphql: entries/graphql-api.md
  data-layer: entries/data-layer.md
  signing: entries/signing.md
  services: entries/services.md
  auth: entries/authentication.md
```

## Reference Docs

```yaml
references:
  overview: references/overview.md
  ingestion: references/ingestion-pipeline.md
  graphql: references/graphql-api.md
  data_layer: references/data-layer.md
  signing: references/signing-and-channels.md
  services: references/services.md
  auth: references/authentication.md
```
