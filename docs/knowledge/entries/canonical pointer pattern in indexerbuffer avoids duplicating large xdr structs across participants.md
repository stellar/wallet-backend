---
description: Transaction structs are 10-50KB each; multiple participants point to same canonical pointer in txByHash; sync.RWMutex protects public methods; Clear() preserves allocated arrays to reduce GC
type: pattern
status: current
confidence: proven
subsystem: ingestion
areas: [ingestion, indexer, memory-management, performance, go]
---

# Canonical pointer pattern in IndexerBuffer avoids duplicating large XDR structs across participants

## Context

A single transaction with many participants (addresses involved) would need to be stored once per participant in a naive implementation. Stellar transactions contain XDR-encoded data (operations, metadata, envelopes) that can reach 10-50KB per transaction.

## Detail

`IndexerBuffer` uses a two-layer architecture:

**Layer 1 (canonical storage):**
- `txByHash map[hash]*Transaction` — one pointer per unique transaction
- `opByID map[id]*Operation` — one pointer per unique operation

**Layer 2 (participant mappings):**
- `participantsByToID map[toID]Set[string]` — Stellar addresses per transaction
- `participantsByOpID map[opID]Set[string]` — Stellar addresses per operation

Multiple participants all reference the same `*Transaction` pointer from Layer 1 — no duplication of XDR data. The pointer is shared across all participant entries.

`sync.RWMutex` protects public `Add*` and `Get*` methods since per-transaction buffers can be merged concurrently by the indexer pool.

`Clear()` resets all maps/slices but preserves the backing arrays, avoiding GC pressure during backfill where the same buffer is reused across many batches.

## Implications

- Never store modified copies of `*Transaction` or `*Operation` — always use the pointer from the canonical maps.
- The two-layer design means "get all participants for transaction X" is a separate lookup from "get transaction X data" — callers need to join Layer 1 and Layer 2 explicitly.
- `Merge()` copies pointers, not values — merging two buffers is cheap regardless of transaction sizes.

## Source

`internal/indexer/indexer_buffer.go` — full two-layer implementation
`internal/indexer/indexer.go:processTransaction()` — usage pattern

## Related

The per-transaction buffers created by [[indexer processes transactions in parallel within a ledger using pond pool with per-transaction buffers]] each have their own canonical pointer maps — the memory efficiency here matters precisely because there are many such buffers active simultaneously during heavy ledgers.

The dedup maps described in [[dedup maps use highest-operationid-wins semantics to resolve intra-ledger state conflicts]] are the Layer 2 component of this two-layer architecture — both entries describe different aspects of IndexerBuffer, with the canonical pointer pattern handling memory and dedup maps handling correctness.

relevant_notes:
  - "[[indexer processes transactions in parallel within a ledger using pond pool with per-transaction buffers]] — grounds this: per-transaction buffer isolation is why canonical pointers matter; each goroutine holds its own buffer with shared-pointer semantics"
  - "[[dedup maps use highest-operationid-wins semantics to resolve intra-ledger state conflicts]] — extends this: dedup maps are Layer 2 of the same IndexerBuffer data structure; canonical pointers (Layer 1) enable memory sharing while dedup maps handle state correctness"
