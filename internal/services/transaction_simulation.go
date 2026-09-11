package services

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// Sentinel errors the GraphQL resolver maps onto extensions.code values.
var (
	// ErrInvalidTransactionXDR means the transaction envelope failed to decode.
	ErrInvalidTransactionXDR = errors.New("invalid transaction XDR")
	// ErrUnsupportedTransaction means the transaction (or one of its operations)
	// is not supported by the implemented simulation sources.
	ErrUnsupportedTransaction = errors.New("unsupported transaction")
)

// SimulatedStateChanges is the result of simulating an unsubmitted transaction:
// the state changes the indexer would produce if it were submitted, evaluated
// against LatestLedger.
type SimulatedStateChanges struct {
	LatestLedger uint32
	StateChanges []types.StateChange
}

// TransactionSimulationService previews the state changes an unsubmitted
// transaction would produce, by rebuilding the transaction's ledger-entry
// changes (contract transactions via RPC simulateTransaction) and running them
// through the same ingestion processors that produce the history API's state
// changes — in memory, without persisting.
type TransactionSimulationService interface {
	SimulateStateChanges(ctx context.Context, transactionXDR string) (*SimulatedStateChanges, error)
}

type transactionSimulationService struct {
	rpcService    RPCService
	ledgerIndexer *indexer.Indexer
}

var _ TransactionSimulationService = (*transactionSimulationService)(nil)

func NewTransactionSimulationService(rpcService RPCService, networkPassphrase string) (*transactionSimulationService, error) {
	if rpcService == nil {
		return nil, errors.New("rpcService cannot be nil")
	}
	// The indexer runs with nil ingestion metrics: simulation is a read-only
	// preview, not ingestion, so it must not feed the ingestion dashboards.
	ledgerIndexer, err := indexer.NewIndexer(networkPassphrase, pond.NewPool(runtime.NumCPU()), nil)
	if err != nil {
		return nil, fmt.Errorf("creating indexer: %w", err)
	}
	return &transactionSimulationService{
		rpcService:    rpcService,
		ledgerIndexer: ledgerIndexer,
	}, nil
}

func (s *transactionSimulationService) SimulateStateChanges(ctx context.Context, transactionXDR string) (*SimulatedStateChanges, error) {
	var envelope xdr.TransactionEnvelope
	if err := xdr.SafeUnmarshalBase64(transactionXDR, &envelope); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTransactionXDR, err)
	}
	if len(envelope.Operations()) == 0 {
		return nil, fmt.Errorf("%w: transaction has no operations", ErrInvalidTransactionXDR)
	}

	if !isSorobanTransaction(envelope) {
		return nil, fmt.Errorf("%w: classic transactions are not supported", ErrUnsupportedTransaction)
	}

	// TODO(#618, phase 1): call rpcService.SimulateTransaction, map its
	// stateChanges/events into a synthesized ingest.LedgerTransaction, and run
	// it through stateChangesForTransaction.
	return nil, fmt.Errorf("%w: contract transaction simulation is not implemented yet", ErrUnsupportedTransaction)
}

// isSorobanTransaction reports whether the envelope carries a Soroban operation
// (InvokeHostFunction, ExtendFootprintTtl, or RestoreFootprint). A Soroban
// transaction has exactly one operation, so inspecting the first is enough.
func isSorobanTransaction(envelope xdr.TransactionEnvelope) bool {
	switch envelope.Operations()[0].Body.Type {
	case xdr.OperationTypeInvokeHostFunction, xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
		return true
	default:
		return false
	}
}

// stateChangesForTransaction runs a synthesized ledger transaction through the
// ingestion pipeline into an in-memory buffer — the same processors real
// ingestion uses, with no persistence — and returns the resulting state changes.
func (s *transactionSimulationService) stateChangesForTransaction(ctx context.Context, tx ingest.LedgerTransaction) ([]types.StateChange, error) {
	buffer := indexer.NewIndexerBuffer()
	if _, err := s.ledgerIndexer.ProcessLedgerTransactions(ctx, []ingest.LedgerTransaction{tx}, buffer); err != nil {
		return nil, fmt.Errorf("processing transaction through indexer: %w", err)
	}
	return buffer.GetStateChanges(), nil
}
