package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/utils"
)

type RPCIngestManager struct {
	RPCService       RPCService
	PaymentModel     *data.PaymentModel
	AppTracker       apptracker.AppTracker
	LedgerCursorName string
	RPCCursorName    string
}

type IngestManager struct {
	PaymentModel      *data.PaymentModel
	LedgerBackend     ledgerbackend.LedgerBackend
	NetworkPassphrase string
	LedgerCursorName  string
	AppTracker        apptracker.AppTracker
}

/*
func (m *RPCIngestManager) Run(ctx context.Context, startLedger uint32, startCursor string) error {
	heartbeat := make(chan any)
	go trackServiceHealth(heartbeat, m.AppTracker)
	cursor := ""
	ledger := 0
	if startCursor != "" {
		cursor = startCursor
	} else if startLedger != 0 {
		ledger = int(startLedger)
	} else {
		lastSyncedCursor, err := m.PaymentModel.GetLatestLedgerSynced(ctx, m.RPCCursorName)
		if err != nil {
			return fmt.Errorf("getting last cursor synced: %w", err)
		}
		if lastSyncedCursor == 0 {
			lastSyncedLedger, err := m.PaymentModel.GetLatestLedgerSynced(ctx, m.LedgerCursorName)
			if err != nil {
				return fmt.Errorf("getting last ledger synced: %w", err)
			}
			ledger = int(lastSyncedLedger)
		}
		cursor = strconv.FormatUint(uint64(lastSyncedCursor), 10)
	}

	for {
		time.Sleep(10)
		txns, cursor, err := m.TransactionService.GetTransactions(ledger, cursor, 200)
		if err != nil {
			return fmt.Errorf("getTransactions: %w", err)
		}
		heartbeat <- true
		iCursor, err := strconv.ParseUint(cursor, 10, 32)
		if err != nil {
			return fmt.Errorf("cannot convert cursor to int: %s", err.Error())
		}
		err = m.PaymentModel.UpdateLatestLedgerSynced(ctx, m.RPCCursorName, uint32(iCursor))
		if err != nil {
			return err
		}
		m.processGetTransactionsResponse(ctx, txns)
	}
}
*/

// func (m *IngestManager) Run(ctx context.Context, start, end uint32) error {
type IngestService interface {
	Run(ctx context.Context, start, end uint32) error
}

var _ IngestService = (*ingestService)(nil)

type ingestService struct {
	models            *data.Models
	ledgerBackend     ledgerbackend.LedgerBackend
	networkPassphrase string
	ledgerCursorName  string
	appTracker        apptracker.AppTracker
	rpcService        RPCService
}

func NewIngestService(
	models *data.Models,
	ledgerBackend ledgerbackend.LedgerBackend,
	networkPassphrase string,
	ledgerCursorName string,
	appTracker apptracker.AppTracker,
	rpcService RPCService,
) (*ingestService, error) {
	if models == nil {
		return nil, errors.New("models cannot be nil")
	}
	if ledgerBackend == nil {
		return nil, errors.New("ledgerBackend cannot be nil")
	}
	if networkPassphrase == "" {
		return nil, errors.New("networkPassphrase cannot be nil")
	}
	if ledgerCursorName == "" {
		return nil, errors.New("ledgerCursorName cannot be nil")
	}
	if appTracker == nil {
		return nil, errors.New("appTracker cannot be nil")
	}
	if rpcService == nil {
		return nil, errors.New("rpcService cannot be nil")
	}

	return &ingestService{
		models:            models,
		ledgerBackend:     ledgerBackend,
		networkPassphrase: networkPassphrase,
		ledgerCursorName:  ledgerCursorName,
		appTracker:        appTracker,
		rpcService:        rpcService,
	}, nil
}

func (m *ingestService) Run(ctx context.Context, start, end uint32) error {
	var ingestLedger uint32

	if start == 0 {
		lastSyncedLedger, err := m.models.Payments.GetLatestLedgerSynced(ctx, m.ledgerCursorName)
		if err != nil {
			return fmt.Errorf("getting last ledger synced: %w", err)
		}

		if lastSyncedLedger == 0 {
			// Captive Core is not able to process genesis ledger (1) and often has trouble processing ledger 2, so we start ingestion at ledger 3
			log.Ctx(ctx).Info("No last synced ledger cursor found, initializing ingestion at ledger 3")
			ingestLedger = 3
		} else {
			ingestLedger = lastSyncedLedger + 1
		}
	} else {
		ingestLedger = start
	}

	if end != 0 && ingestLedger > end {
		return fmt.Errorf("starting ledger (%d) may not be greater than ending ledger (%d)", ingestLedger, end)
	}

	err := m.maybePrepareRange(ctx, ingestLedger, end)
	if err != nil {
		return fmt.Errorf("preparing range from %d to %d: %w", ingestLedger, end, err)
	}

	heartbeat := make(chan any)
	go trackServiceHealth(heartbeat, m.appTracker)

	for ; end == 0 || ingestLedger <= end; ingestLedger++ {
		log.Ctx(ctx).Infof("waiting for ledger %d", ingestLedger)

		ledgerMeta, err := m.ledgerBackend.GetLedger(ctx, ingestLedger)
		if err != nil {
			return fmt.Errorf("getting ledger meta for ledger %d: %w", ingestLedger, err)
		}

		heartbeat <- true

		err = m.processLedger(ctx, ingestLedger, ledgerMeta)
		if err != nil {
			return fmt.Errorf("processing ledger %d: %w", ingestLedger, err)
		}

		log.Ctx(ctx).Infof("ledger %d successfully processed", ingestLedger)
	}

	return nil
}

func (m *ingestService) maybePrepareRange(ctx context.Context, from, to uint32) error {
	var ledgerRange ledgerbackend.Range
	if to == 0 {
		ledgerRange = ledgerbackend.UnboundedRange(from)
	} else {
		ledgerRange = ledgerbackend.BoundedRange(from, to)
	}

	prepared, err := m.ledgerBackend.IsPrepared(ctx, ledgerRange)
	if err != nil {
		return fmt.Errorf("checking prepared range: %w", err)
	}

	if !prepared {
		err = m.ledgerBackend.PrepareRange(ctx, ledgerRange)
		if err != nil {
			return fmt.Errorf("preparing range: %w", err)
		}
	}

	return nil
}

func trackServiceHealth(heartbeat chan any, tracker apptracker.AppTracker) {
	const alertAfter = time.Second * 60
	ticker := time.NewTicker(alertAfter)

	for {
		select {
		case <-ticker.C:
			warn := fmt.Sprintf("ingestion service stale for over %s", alertAfter)
			log.Warn(warn)
			if tracker != nil {
				tracker.CaptureMessage(warn)
			} else {
				log.Warn("App Tracker is nil")
			}
			ticker.Reset(alertAfter)
		case <-heartbeat:
			ticker.Reset(alertAfter)
		}
	}
}

/*
func (m *RPCIngestManager) processGetTransactionsResponse(ctx context.Context, txns []tss.RPCGetIngestTxResponse) (err error) {
	return db.RunInTransaction(ctx, m.PaymentModel.DB, nil, func(dbTx db.Transaction) error {
		for _, tx := range txns {
			if tx.Status != tss.SuccessStatus {
				continue
			}

			genericTx, err := txnbuild.TransactionFromXDR(tx.EnvelopeXDR)
			if err != nil {
				return fmt.Errorf("deserializing envelope xdr: %w", err)
			}
			txEnvelopeXDR, err := genericTx.ToXDR()
			if err != nil {
				return fmt.Errorf("generic transaction cannot be unpacked into a transaction")
			}
			txResultXDR, err := m.TransactionService.UnmarshalTransactionResultXDR(tx.ResultXDR)
			if err != nil {
				return fmt.Errorf("cannot unmarshal transacation result xdr: %s", err.Error())
			}

			txHash := ""

			txMemo, txMemoType := utils.Memo(txEnvelopeXDR.Memo(), "")
			if txMemo != nil {
				*txMemo = utils.SanitizeUTF8(*txMemo)
			}
			txEnvelopeXDR.SourceAccount()
			for idx, op := range txEnvelopeXDR.Operations() {
				opIdx := idx + 1

				payment := data.Payment{
					OperationID:     utils.OperationID(int32(tx.Ledger), int32(tx.ApplicationOrder), int32(opIdx)),
					OperationType:   op.Body.Type.String(),
					TransactionID:   utils.TransactionID(int32(tx.Ledger), int32(tx.ApplicationOrder)),
					TransactionHash: txHash,
					FromAddress:     utils.SourceAccountRPC(op, txEnvelopeXDR),
					CreatedAt:       time.Unix(tx.CreatedAt, 0),
					Memo:            txMemo,
					MemoType:        txMemoType,
				}

				switch op.Body.Type {
				case xdr.OperationTypePayment:
					fillPayment(&payment, op.Body)
				case xdr.OperationTypePathPaymentStrictSend:
					fillPathSendRPC(&payment, op.Body, txResultXDR, opIdx)
				case xdr.OperationTypePathPaymentStrictReceive:
					fillPathReceiveRPC(&payment, op.Body, txResultXDR, opIdx)
				default:
					continue
				}
			}

		}
		return nil
	})
}
*/

func (m *IngestManager) processLedger(ctx context.Context, ledger uint32, ledgerMeta xdr.LedgerCloseMeta) (err error) {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(m.NetworkPassphrase, ledgerMeta)
	if err != nil {
		return fmt.Errorf("creating ledger reader: %w", err)
	}

	ledgerCloseTime := time.Unix(int64(ledgerMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0).UTC()
	ledgerSequence := ledgerMeta.LedgerSequence()

	return db.RunInTransaction(ctx, m.models.Payments.DB, nil, func(dbTx db.Transaction) error {
		for {
			tx, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("reading transaction: %w", err)
			}

			if !tx.Result.Successful() {
				continue
			}

			// tx = deserialized envelopeXdr tx.Envelope.Memo() is now envelope.Memo()
			// tx.Index = applicationOrder - GetTransactions has to return this. May have to update GetTransaction to get this too
			// cant I just use the transaction hash here?
			txHash := utils.TransactionHash(ledgerMeta, int(tx.Index))
			txMemo, txMemoType := utils.Memo(tx.Envelope.Memo(), txHash)
			// The memo field is subject to user input, so we sanitize before persisting in the database
			if txMemo != nil {
				*txMemo = utils.SanitizeUTF8(*txMemo)
			}

			for idx, op := range tx.Envelope.Operations() {
				opIdx := idx + 1

				payment := data.Payment{
					OperationID:     utils.OperationID(int32(ledgerSequence), int32(tx.Index), int32(opIdx)),
					OperationType:   op.Body.Type.String(),
					TransactionID:   utils.TransactionID(int32(ledgerSequence), int32(tx.Index)),
					TransactionHash: txHash,
					FromAddress:     utils.SourceAccount(op, tx),
					CreatedAt:       ledgerCloseTime,
					Memo:            txMemo,
					MemoType:        txMemoType,
				}

				switch op.Body.Type {
				case xdr.OperationTypePayment:
					fillPayment(&payment, op.Body)
				case xdr.OperationTypePathPaymentStrictSend:
					fillPathSend(&payment, op.Body, tx, opIdx)
				case xdr.OperationTypePathPaymentStrictReceive:
					fillPathReceive(&payment, op.Body, tx, opIdx)
				default:
					continue
				}

				err = m.models.Payments.AddPayment(ctx, dbTx, payment)
				if err != nil {
					return fmt.Errorf("adding payment for ledger %d, tx %s (%d), operation %s (%d): %w", ledgerSequence, txHash, tx.Index, payment.OperationID, opIdx, err)
				}
			}
		}

		err = m.models.Payments.UpdateLatestLedgerSynced(ctx, m.ledgerCursorName, ledger)
		if err != nil {
			return err
		}

		return nil
	})
}

func fillPayment(payment *data.Payment, operation xdr.OperationBody) {
	paymentOp := operation.MustPaymentOp()
	payment.ToAddress = paymentOp.Destination.Address()
	payment.SrcAssetCode = utils.AssetCode(paymentOp.Asset)
	payment.SrcAssetIssuer = paymentOp.Asset.GetIssuer()
	payment.SrcAssetType = paymentOp.Asset.Type.String()
	payment.SrcAmount = int64(paymentOp.Amount)
	payment.DestAssetCode = payment.SrcAssetCode
	payment.DestAssetIssuer = payment.SrcAssetIssuer
	payment.DestAssetType = payment.SrcAssetType
	payment.DestAmount = payment.SrcAmount
}

func fillPathSendRPC(payment *data.Payment, operation xdr.OperationBody, txResult xdr.TransactionResult, operationIdx int) {
	pathOp := operation.MustPathPaymentStrictSendOp()
	result := utils.OperationResultRPC(txResult, operationIdx).MustPathPaymentStrictSendResult()
	payment.ToAddress = pathOp.Destination.Address()
	payment.SrcAssetCode = utils.AssetCode(pathOp.SendAsset)
	payment.SrcAssetIssuer = pathOp.SendAsset.GetIssuer()
	payment.SrcAssetType = pathOp.SendAsset.Type.String()
	payment.SrcAmount = int64(pathOp.SendAmount)
	payment.DestAssetCode = utils.AssetCode(pathOp.DestAsset)
	payment.DestAssetIssuer = pathOp.DestAsset.GetIssuer()
	payment.DestAssetType = pathOp.DestAsset.Type.String()
	payment.DestAmount = int64(result.DestAmount())
}

func fillPathSend(payment *data.Payment, operation xdr.OperationBody, transaction ingest.LedgerTransaction, operationIdx int) {
	pathOp := operation.MustPathPaymentStrictSendOp()
	result := utils.OperationResult(transaction, operationIdx).MustPathPaymentStrictSendResult()
	payment.ToAddress = pathOp.Destination.Address()
	payment.SrcAssetCode = utils.AssetCode(pathOp.SendAsset)
	payment.SrcAssetIssuer = pathOp.SendAsset.GetIssuer()
	payment.SrcAssetType = pathOp.SendAsset.Type.String()
	payment.SrcAmount = int64(pathOp.SendAmount)
	payment.DestAssetCode = utils.AssetCode(pathOp.DestAsset)
	payment.DestAssetIssuer = pathOp.DestAsset.GetIssuer()
	payment.DestAssetType = pathOp.DestAsset.Type.String()
	payment.DestAmount = int64(result.DestAmount())
}

func fillPathReceiveRPC(payment *data.Payment, operation xdr.OperationBody, txResult xdr.TransactionResult, operationIdx int) {
	pathOp := operation.MustPathPaymentStrictReceiveOp()
	result := utils.OperationResultRPC(txResult, operationIdx).MustPathPaymentStrictReceiveResult()
	payment.ToAddress = pathOp.Destination.Address()
	payment.SrcAssetCode = utils.AssetCode(pathOp.SendAsset)
	payment.SrcAssetIssuer = pathOp.SendAsset.GetIssuer()
	payment.SrcAssetType = pathOp.SendAsset.Type.String()
	payment.SrcAmount = int64(result.SendAmount())
	payment.DestAssetCode = utils.AssetCode(pathOp.DestAsset)
	payment.DestAssetIssuer = pathOp.DestAsset.GetIssuer()
	payment.DestAssetType = pathOp.DestAsset.Type.String()
	payment.DestAmount = int64(pathOp.DestAmount)
}

func fillPathReceive(payment *data.Payment, operation xdr.OperationBody, transaction ingest.LedgerTransaction, operationIdx int) {
	pathOp := operation.MustPathPaymentStrictReceiveOp()
	result := utils.OperationResult(transaction, operationIdx).MustPathPaymentStrictReceiveResult()
	payment.ToAddress = pathOp.Destination.Address()
	payment.SrcAssetCode = utils.AssetCode(pathOp.SendAsset)
	payment.SrcAssetIssuer = pathOp.SendAsset.GetIssuer()
	payment.SrcAssetType = pathOp.SendAsset.Type.String()
	payment.SrcAmount = int64(result.SendAmount())
	payment.DestAssetCode = utils.AssetCode(pathOp.DestAsset)
	payment.DestAssetIssuer = pathOp.DestAsset.GetIssuer()
	payment.DestAssetType = pathOp.DestAsset.Type.String()
	payment.DestAmount = int64(pathOp.DestAmount)
}
