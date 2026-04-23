package services

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
)

// ProtocolProcessor produces and persists protocol-specific state for a ledger.
type ProtocolProcessor interface {
	ProtocolID() string
	// ProcessLedger analyzes a ledger and stages any per-ledger protocol state
	// needed by PersistHistory/PersistCurrentState. It runs before any
	// transaction-scoped LoadCurrentState reload for the ledger, and it may be
	// called more than once for the same ledger when persistence retries, so
	// implementations must deterministically rebuild staged state from the
	// provided input.
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
	// provided transaction and updates the processor's in-memory cache to
	// reflect the writes (write-through). Called inside the ledger's DB
	// transaction only when the current_state cursor CAS advances for this
	// ledger, and only after LoadCurrentState has hydrated the cache on
	// first CAS success. If the transaction rolls back after this runs,
	// the caller resets the loaded flag to force a reload on the next
	// attempt, since the in-memory cache may be ahead of committed DB
	// state. Implementations must mutate the cache only via writes that
	// also go through dbTx so committed state and cached state stay in
	// sync on commit.
	PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error
	// LoadCurrentState reads the protocol's current state from its state tables
	// into processor memory. Called inside the DB transaction on the first
	// successful CAS advance of the current_state_cursor (the handoff moment
	// from migration to live ingestion). It may be called again after a
	// rollback resets the loaded flag, so implementations must be safe to
	// invoke multiple times — each call should fully replace in-memory state
	// from the DB. Between successful loads, subsequent ledgers use the
	// in-memory state maintained by PersistCurrentState.
	LoadCurrentState(ctx context.Context, dbTx pgx.Tx) error
}

// ProtocolProcessorInput contains the data needed by a processor to analyze a ledger.
type ProtocolProcessorInput struct {
	LedgerSequence    uint32
	LedgerCloseMeta   xdr.LedgerCloseMeta
	ProtocolContracts []data.ProtocolContracts
	NetworkPassphrase string
}
