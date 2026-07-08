package services

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
)

// StagingMode tells a processor which staged sets to build. The caller stamps it
// each ledger: the migration engine sets it from its strategy; live ingestion
// sets StagingModeBoth. The zero value ("") behaves as Both — build everything.
type StagingMode string

const (
	StagingModeHistory      StagingMode = "history"
	StagingModeCurrentState StagingMode = "current_state"
	StagingModeBoth         StagingMode = "both"
)

// NeedsHistory reports whether history rows should be staged.
func (m StagingMode) NeedsHistory() bool { return m != StagingModeCurrentState }

// NeedsCurrentState reports whether current-state (balances/allowances) should be staged.
func (m StagingMode) NeedsCurrentState() bool { return m != StagingModeHistory }

// ProtocolProcessor produces and persists protocol-specific state for a ledger.
type ProtocolProcessor interface {
	ProtocolID() string
	// ProcessLedger accumulates ("folds") this ledger's protocol state into the
	// processor's staged sets. It does NOT reset between ledgers — the caller owns
	// reset via Reset(). Folding a window of ledgers then calling a Persist method
	// MUST produce the same result as processing and persisting each ledger
	// individually (batch-equivalence): balances sum, last-write-wins values keep
	// the newest, history rows append. A protocol that cannot meet this must be
	// migrated with --window-size=1.
	ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error

	// RequiresContractData reports whether ProcessLedger needs
	// ProtocolProcessorInput.ContractDataChanges populated. The migration
	// engine and live ingestion run the (heavier) ContractData extraction
	// only when a selected processor returns true, so event-only protocols
	// pay nothing for it.
	RequiresContractData() bool

	// Reset clears the staged sets after a window commits or hands off. The caller
	// (engine per window; live ingestion per ledger) invokes it.
	Reset()

	// PersistHistory writes the history rows accumulated by ProcessLedger since the
	// last Reset, using the provided transaction. Called inside the CAS-guarded
	// transaction only when the cursor advances, so writes commit atomically with
	// the cursor update and any failure rolls back both.
	PersistHistory(ctx context.Context, dbTx pgx.Tx) error
	// PersistCurrentState writes the protocol's current state accumulated by
	// ProcessLedger since the last Reset, using the provided transaction. Called
	// inside the CAS-guarded transaction only when the cursor advances, so writes
	// commit atomically with the cursor update and any failure rolls back both.
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
	// StagingMode selects which staged sets the processor builds for this ledger.
	StagingMode StagingMode
}
