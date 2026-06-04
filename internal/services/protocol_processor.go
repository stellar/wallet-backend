package services

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
)

// ProtocolProcessor produces and persists protocol-specific state for a ledger.
type ProtocolProcessor interface {
	ProtocolID() string
	// ProcessLedger analyzes a ledger and stages any per-ledger protocol state
	// needed by PersistHistory/PersistCurrentState. It may be called more than
	// once for the same ledger when persistence retries, so implementations
	// must deterministically rebuild staged state from the provided input.
	ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error
	// PersistHistory writes the history rows staged by ProcessLedger using the
	// provided transaction. Called inside the ledger's DB transaction only
	// when the history cursor CAS advances for this ledger, so writes commit
	// atomically with the cursor update and any failure rolls back both. On
	// rollback the next retry re-stages via ProcessLedger and calls
	// PersistHistory again, so implementations must not depend on in-memory
	// state surviving across invocations.
	PersistHistory(ctx context.Context, dbTx pgx.Tx) error
	// PersistCurrentState writes the protocol's current state using the
	// provided transaction. Called inside the ledger's DB transaction only
	// when the current_state cursor CAS advances for this ledger, so writes
	// commit atomically with the cursor update and any failure rolls back
	// both. On rollback the next retry re-stages via ProcessLedger and calls
	// PersistCurrentState again, so implementations must not depend on
	// in-memory state surviving across invocations.
	PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error
}

// ProtocolProcessorInput contains the data needed by a processor to analyze a ledger.
//
// ContractEvents is keyed by (txIdx, opIdx) and contains events only from
// successful transactions — the indexer filters at extraction time
// (internal/indexer/indexer.go in processTransaction). Each protocol processor
// is expected to iterate the map and apply its own contract filter
// (e.g. SEP-41 ignores events from contracts it doesn't track).
type ProtocolProcessorInput struct {
	LedgerSequence    uint32
	LedgerCloseTime   int64
	ContractEvents    map[indexer.ContractEventKey][]xdr.ContractEvent
	ProtocolContracts []data.ProtocolContracts
}
