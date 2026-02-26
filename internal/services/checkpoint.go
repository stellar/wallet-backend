package services

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/db"
)

// CheckpointService orchestrates checkpoint population by coordinating
// token and WASM ingestion services.
type CheckpointService interface {
	PopulateFromCheckpoint(ctx context.Context, checkpointLedger uint32, initializeCursors func(pgx.Tx) error) error
}

var _ CheckpointService = (*checkpointService)(nil)

// readerFactory creates a ChangeReader for a given checkpoint ledger.
type readerFactory func(ctx context.Context, archive historyarchive.ArchiveInterface, checkpointLedger uint32) (ingest.ChangeReader, error)

// defaultReaderFactory wraps ingest.NewCheckpointChangeReader to satisfy readerFactory.
func defaultReaderFactory(ctx context.Context, archive historyarchive.ArchiveInterface, checkpointLedger uint32) (ingest.ChangeReader, error) {
	return ingest.NewCheckpointChangeReader(ctx, archive, checkpointLedger)
}

type checkpointService struct {
	db                    db.ConnectionPool
	archive               historyarchive.ArchiveInterface
	tokenIngestionService TokenIngestionService
	wasmIngestionService  WasmIngestionService
	contractValidator     ContractValidator
	readerFactory         readerFactory
}

// NewCheckpointService creates a CheckpointService.
func NewCheckpointService(
	dbPool db.ConnectionPool,
	archive historyarchive.ArchiveInterface,
	tokenIngestionService TokenIngestionService,
	wasmIngestionService WasmIngestionService,
	contractValidator ContractValidator,
) CheckpointService {
	return &checkpointService{
		db:                    dbPool,
		archive:               archive,
		tokenIngestionService: tokenIngestionService,
		wasmIngestionService:  wasmIngestionService,
		contractValidator:     contractValidator,
		readerFactory:         defaultReaderFactory,
	}
}

// PopulateFromCheckpoint performs initial cache population from Stellar history archive.
// It creates a checkpoint reader, iterates all entries in a single pass, delegating to
// the token and WASM services, then finalizes both within a single DB transaction.
func (s *checkpointService) PopulateFromCheckpoint(ctx context.Context, checkpointLedger uint32, initializeCursors func(pgx.Tx) error) error {
	if s.archive == nil {
		return fmt.Errorf("history archive not configured - PopulateFromCheckpoint requires archive connection")
	}

	defer func() {
		if err := s.contractValidator.Close(ctx); err != nil {
			log.Ctx(ctx).Errorf("error closing contract spec validator: %v", err)
		}
	}()

	log.Ctx(ctx).Infof("Populating from checkpoint ledger = %d", checkpointLedger)

	// Create checkpoint change reader
	reader, err := s.readerFactory(ctx, s.archive, checkpointLedger)
	if err != nil {
		return fmt.Errorf("creating checkpoint change reader: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Ctx(ctx).Errorf("error closing checkpoint reader: %v", closeErr)
		}
	}()

	// Wrap ALL DB operations in a single transaction for atomicity
	err = db.RunInPgxTransaction(ctx, s.db, func(dbTx pgx.Tx) error {
		// Disable synchronous commit for this transaction only - safe for checkpoint
		// population since it's idempotent and can be re-run if crash occurs
		if _, txErr := dbTx.Exec(ctx, "SET LOCAL synchronous_commit = off"); txErr != nil {
			return fmt.Errorf("setting synchronous_commit=off: %w", txErr)
		}

		// Create the token checkpoint processor
		tokenProcessor := s.tokenIngestionService.NewTokenProcessor(dbTx, checkpointLedger)

		// Single-pass iteration over all checkpoint entries
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("checkpoint processing cancelled: %w", ctx.Err())
			default:
			}

			change, readErr := reader.Read()
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				return fmt.Errorf("reading checkpoint changes: %w", readErr)
			}

			// ContractCode entries go to BOTH services
			if change.Type == xdr.LedgerEntryTypeContractCode {
				contractCodeEntry := change.Post.Data.MustContractCode()

				// WASM service: track hash for protocol_wasms
				if txErr := s.wasmIngestionService.ProcessContractCode(ctx, contractCodeEntry.Hash, contractCodeEntry.Code); txErr != nil {
					return fmt.Errorf("wasm service processing contract code: %w", txErr)
				}

				// Token service: SEP-41 validation for token identification
				if txErr := tokenProcessor.ProcessContractCode(ctx, contractCodeEntry.Hash, contractCodeEntry.Code); txErr != nil {
					return fmt.Errorf("token processor processing contract code: %w", txErr)
				}
			} else {
				// Account, Trustline, ContractData entries → token processor
				if txErr := tokenProcessor.ProcessEntry(ctx, change); txErr != nil {
					return fmt.Errorf("token processor processing entry: %w", txErr)
				}
			}

			// Flush token batches periodically
			if txErr := tokenProcessor.FlushBatchIfNeeded(ctx); txErr != nil {
				return fmt.Errorf("flushing token batch: %w", txErr)
			}
		}

		// Flush remaining token batch
		if txErr := tokenProcessor.FlushRemainingBatch(ctx); txErr != nil {
			return fmt.Errorf("flushing remaining token batch: %w", txErr)
		}

		// Post-processing: finalize tokens (SEP-41 identification + RPC metadata + DB store)
		if txErr := tokenProcessor.Finalize(ctx, dbTx); txErr != nil {
			return fmt.Errorf("finalizing token processor: %w", txErr)
		}

		// Post-processing: persist protocol WASMs
		if txErr := s.wasmIngestionService.PersistProtocolWasms(ctx, dbTx); txErr != nil {
			return fmt.Errorf("persisting protocol wasms: %w", txErr)
		}

		// Initialize cursors within the same transaction
		if txErr := initializeCursors(dbTx); txErr != nil {
			return fmt.Errorf("initializing cursors: %w", txErr)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("running db transaction for checkpoint population: %w", err)
	}
	return nil
}
