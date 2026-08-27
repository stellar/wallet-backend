package services

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
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
	// ErrSimulationFailed means RPC simulateTransaction ran but reported an error,
	// for example the contract call would trap.
	ErrSimulationFailed = errors.New("simulation failed")
)

// SimulatedStateChanges is what a simulation returns: the state changes the
// indexer would produce if this transaction were actually submitted, evaluated
// against the ledger in LatestLedger.
type SimulatedStateChanges struct {
	LatestLedger uint32
	StateChanges []types.StateChange
}

// TransactionSimulationService previews the state changes an unsubmitted
// transaction would produce. It rebuilds the transaction's ledger-entry changes
// (Soroban transactions via RPC simulateTransaction) and runs them through the
// very same processors that build the history API's state changes — all in
// memory, nothing written to the database. The point is for a preview to look
// exactly like history.
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

	// Soroban transactions get ledger-entry changes from RPC simulation. Classic
	// derivation is not implemented yet and returns ErrUnsupportedTransaction.
	// Successful simulation paths then share the synthesis and processing below.
	var (
		tx           ingest.LedgerTransaction
		latestLedger uint32
		err          error
	)
	if isSorobanTransaction(envelope) {
		tx, latestLedger, err = s.ledgerTransactionFromContract(transactionXDR, envelope)
	} else {
		tx, latestLedger, err = s.ledgerTransactionFromClassic(ctx, envelope)
	}
	if err != nil {
		return nil, err
	}

	stateChanges, err := s.stateChangesForTransaction(ctx, tx)
	if err != nil {
		return nil, err
	}
	return &SimulatedStateChanges{
		LatestLedger: latestLedger,
		StateChanges: stateChanges,
	}, nil
}

// ledgerTransactionFromContract builds the simulated ledger transaction for a
// Soroban transaction. It asks RPC to simulate the transaction and turns that
// result into the ledger-entry changes and events the processors expect.
func (s *transactionSimulationService) ledgerTransactionFromContract(transactionXDR string, envelope xdr.TransactionEnvelope) (ingest.LedgerTransaction, uint32, error) {
	result, err := s.rpcService.SimulateTransaction(transactionXDR, entities.RPCResourceConfig{})
	if err != nil {
		return ingest.LedgerTransaction{}, 0, fmt.Errorf("simulating transaction via RPC: %w", err)
	}
	if result.Error != "" {
		return ingest.LedgerTransaction{}, 0, fmt.Errorf("%w: %s", ErrSimulationFailed, result.Error)
	}

	tx, err := buildSimulatedLedgerTransaction(envelope, result)
	if err != nil {
		return ingest.LedgerTransaction{}, 0, fmt.Errorf("synthesizing ledger transaction: %w", err)
	}
	return tx, uint32(result.LatestLedger), nil
}

// ledgerTransactionFromClassic will build the simulated ledger transaction for a
// classic transaction. RPC can't simulate classic operations, so we derive the
// changes ourselves: work out which ledger entries the operation touches, fetch
// their current state with GetLedgerEntries, then compute what they would become.
// Not implemented yet.
//
//nolint:unparam // stub: returns a nil LedgerTransaction until classic derivation is implemented.
func (s *transactionSimulationService) ledgerTransactionFromClassic(_ context.Context, _ xdr.TransactionEnvelope) (ingest.LedgerTransaction, uint32, error) {
	return ingest.LedgerTransaction{}, 0, fmt.Errorf("%w: classic transactions are not supported yet", ErrUnsupportedTransaction)
}

// buildSimulatedLedgerTransaction turns an RPC simulateTransaction result into a
// LedgerTransaction that looks just like one from a real ledger, so the existing
// processors can read it without knowing it came from a simulation. It puts the
// simulation's before/after ledger entries where the processors look for
// per-operation changes, pulls the contract events out of the diagnostic events
// for the token-transfer processor, and marks the transaction as successful at
// the ledger the simulation ran against. Nothing is executed or written.
func buildSimulatedLedgerTransaction(envelope xdr.TransactionEnvelope, result entities.RPCSimulateTransactionResult) (ingest.LedgerTransaction, error) {
	changes, err := ledgerEntryChangesFromSimulation(result.StateChanges)
	if err != nil {
		return ingest.LedgerTransaction{}, fmt.Errorf("building ledger entry changes: %w", err)
	}
	events, err := contractEventsFromSimulation(result.Events)
	if err != nil {
		return ingest.LedgerTransaction{}, fmt.Errorf("extracting contract events: %w", err)
	}
	opResults, err := successOperationResults(envelope)
	if err != nil {
		return ingest.LedgerTransaction{}, err
	}

	// The fee row should reflect what the network would charge: the transaction's
	// inclusion fee plus the resource fee. tx.Fee already bundles the inclusion fee
	// with the resource-fee bid, so subtract the declared bid to isolate the inclusion
	// portion, then add the freshly simulated resource fee. A missing or malformed
	// minResourceFee is a synthesis error rather than a silent zero-fee preview.
	minResourceFee, err := strconv.ParseInt(result.MinResourceFee, 10, 64)
	if err != nil {
		return ingest.LedgerTransaction{}, fmt.Errorf("parsing minResourceFee %q: %w", result.MinResourceFee, err)
	}
	inclusionFee := int64(envelope.Fee()) - envelopeResourceFee(envelope)
	if inclusionFee < 0 {
		inclusionFee = 0
	}
	feeCharged := inclusionFee + minResourceFee

	return ingest.LedgerTransaction{
		Index:    1,
		Envelope: envelope,
		Ledger: xdr.LedgerCloseMeta{
			V: 0,
			V0: &xdr.LedgerCloseMetaV0{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(result.LatestLedger)},
				},
			},
		},
		Result: xdr.TransactionResultPair{
			Result: xdr.TransactionResult{
				FeeCharged: xdr.Int64(feeCharged),
				Result: xdr.TransactionResultResult{
					Code:    xdr.TransactionResultCodeTxSuccess,
					Results: opResults,
				},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{
			V: 3,
			V3: &xdr.TransactionMetaV3{
				Operations:  []xdr.OperationMeta{{Changes: changes}},
				SorobanMeta: &xdr.SorobanTransactionMeta{Events: events},
			},
		},
	}, nil
}

// envelopeResourceFee returns the Soroban resource fee the transaction declares in
// its SorobanData, or 0 if it declares none. tx.Fee already includes this bid, so
// subtracting it leaves the inclusion fee.
func envelopeResourceFee(envelope xdr.TransactionEnvelope) int64 {
	var ext xdr.TransactionExt
	switch envelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		if envelope.V1 != nil {
			ext = envelope.V1.Tx.Ext
		}
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		if envelope.FeeBump != nil && envelope.FeeBump.Tx.InnerTx.V1 != nil {
			ext = envelope.FeeBump.Tx.InnerTx.V1.Tx.Ext
		}
	default:
		// TxV0 and non-transaction envelope types carry no SorobanData.
	}
	if ext.SorobanData != nil {
		return int64(ext.SorobanData.ResourceFee)
	}
	return 0
}

// ledgerEntryChangesFromSimulation turns the simulation's before/after entries
// into the xdr.LedgerEntryChanges the processors expect: the same
// State/Created/Updated/Removed shape GetOperationChanges hands back as {Pre,
// Post} pairs. Which one it is comes from which side is present. Before only
// means the entry was removed, after only means it was created, both means it
// was updated.
func ledgerEntryChangesFromSimulation(stateChanges []entities.RPCSimulateStateChange) (xdr.LedgerEntryChanges, error) {
	changes := make(xdr.LedgerEntryChanges, 0, len(stateChanges))
	for _, sc := range stateChanges {
		before, err := decodeLedgerEntry(sc.Before)
		if err != nil {
			return nil, fmt.Errorf("decoding before entry: %w", err)
		}
		after, err := decodeLedgerEntry(sc.After)
		if err != nil {
			return nil, fmt.Errorf("decoding after entry: %w", err)
		}

		switch {
		case before == nil && after != nil:
			changes = append(changes, xdr.LedgerEntryChange{
				Type:    xdr.LedgerEntryChangeTypeLedgerEntryCreated,
				Created: after,
			})
		case before != nil && after != nil:
			changes = append(changes,
				xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: before},
				xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: after},
			)
		case before != nil && after == nil:
			var key xdr.LedgerKey
			if err := xdr.SafeUnmarshalBase64(sc.Key, &key); err != nil {
				return nil, fmt.Errorf("decoding removed entry key: %w", err)
			}
			changes = append(changes,
				xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: before},
				xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved, Removed: &key},
			)
		}
	}
	return changes, nil
}

func decodeLedgerEntry(encoded *string) (*xdr.LedgerEntry, error) {
	if encoded == nil {
		return nil, nil
	}
	var entry xdr.LedgerEntry
	if err := xdr.SafeUnmarshalBase64(*encoded, &entry); err != nil {
		return nil, fmt.Errorf("decoding ledger entry: %w", err)
	}
	return &entry, nil
}

// contractEventsFromSimulation pulls the contract events out of the simulation's
// diagnostic events. In real ingestion the token-transfer processor only sees
// the contract's own emitted events (SorobanMeta.Events), so we keep only the
// events that would land there and drop the rest:
//   - diagnostic-type bookkeeping events (fn_call, logs, …), which never reach
//     that processor; and
//   - events from a failed nested call the outer invocation caught
//     (InSuccessfulContractCall=false), which are absent from committed
//     SorobanMeta.Events.
//
// Including either would produce state changes history never shows.
func contractEventsFromSimulation(encoded []string) ([]xdr.ContractEvent, error) {
	events := make([]xdr.ContractEvent, 0, len(encoded))
	for _, e := range encoded {
		var diagnostic xdr.DiagnosticEvent
		if err := xdr.SafeUnmarshalBase64(e, &diagnostic); err != nil {
			return nil, fmt.Errorf("decoding diagnostic event: %w", err)
		}
		if !diagnostic.InSuccessfulContractCall || diagnostic.Event.Type == xdr.ContractEventTypeDiagnostic {
			continue
		}
		events = append(events, diagnostic.Event)
	}
	return events, nil
}

// successOperationResults builds one "succeeded" OperationResult per operation,
// matching each operation's type. The processors look results up by position, so
// the slice has to be there and each entry's type has to line up with the
// matching operation in the envelope.
func successOperationResults(envelope xdr.TransactionEnvelope) (*[]xdr.OperationResult, error) {
	ops := envelope.Operations()
	results := make([]xdr.OperationResult, 0, len(ops))
	for _, op := range ops {
		tr := xdr.OperationResultTr{Type: op.Body.Type}
		switch op.Body.Type {
		case xdr.OperationTypeInvokeHostFunction:
			tr.InvokeHostFunctionResult = &xdr.InvokeHostFunctionResult{
				Code:    xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess,
				Success: &xdr.Hash{},
			}
		case xdr.OperationTypeExtendFootprintTtl:
			tr.ExtendFootprintTtlResult = &xdr.ExtendFootprintTtlResult{
				Code: xdr.ExtendFootprintTtlResultCodeExtendFootprintTtlSuccess,
			}
		case xdr.OperationTypeRestoreFootprint:
			tr.RestoreFootprintResult = &xdr.RestoreFootprintResult{
				Code: xdr.RestoreFootprintResultCodeRestoreFootprintSuccess,
			}
		default:
			return nil, fmt.Errorf("%w: operation type %s", ErrUnsupportedTransaction, op.Body.Type)
		}
		results = append(results, xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &tr})
	}
	return &results, nil
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
