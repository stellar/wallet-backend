# Ingestion Pipeline Architecture

## Live Ingestion Flow

<!-- TODO: Read internal/services/ingest_live.go.
     Trace the flow: startLiveIngestion → ingestLiveLedgers → processLedger.
     Create ASCII diagram showing:
       RPC GetLedgers → Ledger → Indexer.Process → [Processor1, Processor2, ...] → DB Batch Insert
     Document how cursor tracking works for resumability. -->

## Backfill Ingestion Flow

<!-- TODO: Read internal/services/ingest_backfill.go.
     Document the parallel batch processing strategy:
     - How ledger ranges are divided into batches
     - Worker pool / goroutine concurrency model
     - How backfill progress is tracked
     - How backfill interacts with live ingestion (non-overlapping ranges) -->

## Indexer Architecture

<!-- TODO: Read internal/indexer/indexer.go.
     Document the Indexer struct and its Process method.
     List all processors from internal/indexer/processors/ with a 1-line description each.
     Create ASCII diagram:
       Ledger → Indexer.Process()
                  ├── TransactionProcessor → transactions table
                  ├── OperationProcessor → operations table
                  ├── StateChangeProcessor → state_changes table
                  └── ... (list all) -->

## Retry Logic & Cursor Management

<!-- TODO: Read the retry/backoff constants in the ingest services.
     Document:
     - Retry strategy for RPC failures
     - Cursor tracking in IngestStoreModel (read internal/data/ingest_store_model.go)
     - How the system recovers from crashes (cursor-based resumption)
     - Any dead-letter or skip logic for permanently failed ledgers -->

## Ledger Backend

<!-- TODO: Read how the ingester connects to Stellar RPC.
     Document the RPC client configuration and the getLedgers request/response cycle.
     Reference internal/services/rpc_service.go if relevant. -->
