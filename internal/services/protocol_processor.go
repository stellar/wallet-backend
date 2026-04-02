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
	ProcessLedger(ctx context.Context, input ProtocolProcessorInput) error
	PersistHistory(ctx context.Context, dbTx pgx.Tx) error
	PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error
}

// ProtocolProcessorInput contains the data needed by a processor to analyze a ledger.
type ProtocolProcessorInput struct {
	LedgerSequence    uint32
	LedgerCloseMeta   xdr.LedgerCloseMeta
	ProtocolContracts []data.ProtocolContracts
	NetworkPassphrase string
}
