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
	PersistHistory(ctx context.Context, dbTx pgx.Tx) error
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
