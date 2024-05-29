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
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/utils"
)

type IngestManager struct {
	PaymentModel      *data.PaymentModel
	LedgerBackend     ledgerbackend.LedgerBackend
	NetworkPassphrase string
	LedgerCursorName  string
}

func (m *IngestManager) Run(ctx context.Context, start, end uint32) error {
	var ingestLedger uint32

	if start == 0 {
		lastSyncedLedger, err := m.PaymentModel.GetLatestLedgerSynced(ctx, m.LedgerCursorName)
		if err != nil {
			return fmt.Errorf("getting last ledger synced: %w", err)
		}

		if lastSyncedLedger == 0 {
			return errors.New("ingestion service is not initialized, --start flag is required")
		}
		ingestLedger = lastSyncedLedger
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
	go trackServiceHealth(heartbeat)

	for ; end == 0 || ingestLedger <= end; ingestLedger++ {
		log.Ctx(ctx).Infof("waiting for ledger %d", ingestLedger)

		ledgerMeta, err := m.LedgerBackend.GetLedger(ctx, ingestLedger)
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

func (m *IngestManager) maybePrepareRange(ctx context.Context, from, to uint32) error {
	var ledgerRange ledgerbackend.Range
	if to == 0 {
		ledgerRange = ledgerbackend.UnboundedRange(from)
	} else {
		ledgerRange = ledgerbackend.BoundedRange(from, to)
	}

	prepared, err := m.LedgerBackend.IsPrepared(ctx, ledgerRange)
	if err != nil {
		return fmt.Errorf("checking prepared range: %w", err)
	}

	if !prepared {
		err = m.LedgerBackend.PrepareRange(ctx, ledgerRange)
		if err != nil {
			return fmt.Errorf("preparing range: %w", err)
		}
	}

	return nil
}

func trackServiceHealth(heartbeat chan any) {
	const alertAfter = time.Second * 60
	ticker := time.NewTicker(alertAfter)

	for {
		select {
		case <-ticker.C:
			warn := fmt.Sprintf("ingestion service stale for over %s", alertAfter)
			log.Warn(warn)
			// TODO: track in Sentry
			// sentry.CaptureMessage(warn)
			ticker.Reset(alertAfter)
		case <-heartbeat:
			ticker.Reset(alertAfter)
		}
	}
}

func (m *IngestManager) processLedger(ctx context.Context, ledger uint32, ledgerMeta xdr.LedgerCloseMeta) (err error) {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(m.NetworkPassphrase, ledgerMeta)
	if err != nil {
		return fmt.Errorf("creating ledger reader: %w", err)
	}

	ledgerCloseTime := time.Unix(int64(ledgerMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0).UTC()
	ledgerSequence := ledgerMeta.LedgerSequence()

	return db.RunInTransaction(ctx, m.PaymentModel.DB, nil, func(dbTx db.Transaction) error {
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

			txHash := utils.TransactionHash(ledgerMeta, int(tx.Index))
			txMemo := utils.Memo(tx.Envelope.Memo(), txHash)
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
					From:            utils.SourceAccount(op, tx),
					CreatedAt:       ledgerCloseTime,
					Memo:            txMemo,
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

				err = m.PaymentModel.AddPayment(ctx, dbTx, payment)
				if err != nil {
					return fmt.Errorf("adding payment for ledger %d, tx %q (%d), operation %d (%d): %w", ledgerSequence, txHash, tx.Index, payment.OperationID, opIdx, err)
				}
			}
		}

		err = m.PaymentModel.UpdateLatestLedgerSynced(ctx, m.LedgerCursorName, ledger)
		if err != nil {
			return err
		}

		return nil
	})
}

func fillPayment(payment *data.Payment, operation xdr.OperationBody) {
	paymentOp := operation.MustPaymentOp()
	payment.To = paymentOp.Destination.Address()
	payment.SrcAssetCode = utils.AssetCode(paymentOp.Asset)
	payment.DestAssetCode = payment.SrcAssetCode
	payment.SrcAssetIssuer = paymentOp.Asset.GetIssuer()
	payment.DestAssetIssuer = payment.SrcAssetIssuer
	payment.SrcAmount = int64(paymentOp.Amount)
	payment.DestAmount = int64(paymentOp.Amount)
}

func fillPathSend(payment *data.Payment, operation xdr.OperationBody, transaction ingest.LedgerTransaction, operationIdx int) {
	pathOp := operation.MustPathPaymentStrictSendOp()
	result := utils.OperationResult(transaction, operationIdx).MustPathPaymentStrictSendResult()
	payment.To = pathOp.Destination.Address()
	payment.SrcAssetCode = utils.AssetCode(pathOp.SendAsset)
	payment.DestAssetCode = utils.AssetCode(pathOp.DestAsset)
	payment.SrcAssetIssuer = pathOp.SendAsset.GetIssuer()
	payment.DestAssetIssuer = pathOp.DestAsset.GetIssuer()
	payment.SrcAmount = int64(pathOp.SendAmount)
	payment.DestAmount = int64(result.DestAmount())
}

func fillPathReceive(payment *data.Payment, operation xdr.OperationBody, transaction ingest.LedgerTransaction, operationIdx int) {
	pathOp := operation.MustPathPaymentStrictReceiveOp()
	result := utils.OperationResult(transaction, operationIdx).MustPathPaymentStrictReceiveResult()
	payment.To = pathOp.Destination.Address()
	payment.SrcAssetCode = utils.AssetCode(pathOp.SendAsset)
	payment.DestAssetCode = utils.AssetCode(pathOp.DestAsset)
	payment.SrcAssetIssuer = pathOp.SendAsset.GetIssuer()
	payment.DestAssetIssuer = pathOp.DestAsset.GetIssuer()
	payment.SrcAmount = int64(result.SendAmount())
	payment.DestAmount = int64(pathOp.DestAmount)
}
