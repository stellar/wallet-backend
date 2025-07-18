package processors

import (
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func ConvertTransaction(transaction *ingest.LedgerTransaction) (*types.Transaction, error) {
	envelopeXDR, err := xdr.MarshalBase64(transaction.Envelope)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction envelope: %w", err)
	}

	resultXDR, err := xdr.MarshalBase64(transaction.Result)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction result: %w", err)
	}

	metaXDR, err := xdr.MarshalBase64(transaction.UnsafeMeta)
	if err != nil {
		return nil, fmt.Errorf("marshalling transaction meta: %w", err)
	}

	ledgerSequence := transaction.Ledger.LedgerSequence()
	transactionID := toid.New(int32(ledgerSequence), int32(transaction.Index), 0).ToInt64()

	return &types.Transaction{
		ToID:            transactionID,
		Hash:            transaction.Hash.HexString(),
		LedgerCreatedAt: transaction.Ledger.ClosedAt(),
		EnvelopeXDR:     envelopeXDR,
		ResultXDR:       resultXDR,
		MetaXDR:         metaXDR,
		LedgerNumber:    ledgerSequence,
	}, nil
}

func ConvertOperation(transaction *ingest.LedgerTransaction, op *xdr.Operation, opID int64) (*types.Operation, error) {
	xdrOpStr, err := xdr.MarshalBase64(op)
	if err != nil {
		return nil, fmt.Errorf("marshalling operation %d: %w", opID, err)
	}

	return &types.Operation{
		ID:              opID,
		OperationType:   types.OperationTypeFromXDR(op.Body.Type),
		OperationXDR:    xdrOpStr,
		LedgerCreatedAt: transaction.Ledger.ClosedAt(),
		TxHash:          transaction.Hash.HexString(),
	}, nil
}
