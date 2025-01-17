package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/tss"
	tssrouter "github.com/stellar/wallet-backend/internal/tss/router"
	tssstore "github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	ingestHealthCheckMaxWaitTime = 90 * time.Second
	paymentPrometheusLabel               = "payment"
	pathPaymentStrictSendPrometheusLabel       = "path_payment_strict_send"
	pathPaymentStrictReceivePrometheusLabel    = "path_payment_strict_receive"
)

type IngestService interface {
	Run(ctx context.Context, startLedger uint32, endLedger uint32) error
}

var _ IngestService = (*ingestService)(nil)

type ingestService struct {
	models           *data.Models
	ledgerCursorName string
	appTracker       apptracker.AppTracker
	rpcService       RPCService
	tssRouter        tssrouter.Router
	tssStore         tssstore.Store

	// Metrics
	numPaymentOpsIngested *prometheus.CounterVec
	numTssTransactionsIngested *prometheus.CounterVec
	paymentAmountsIngested *prometheus.GaugeVec
}

func NewIngestService(
	models *data.Models,
	ledgerCursorName string,
	appTracker apptracker.AppTracker,
	rpcService RPCService,
	tssRouter tssrouter.Router,
	tssStore tssstore.Store,
	metricsRegistry *prometheus.Registry,
) (*ingestService, error) {
	if models == nil {
		return nil, errors.New("models cannot be nil")
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
	if tssRouter == nil {
		return nil, errors.New("tssRouter cannot be nil")
	}
	if tssStore == nil {
		return nil, errors.New("tssStore cannot be nil")
	}

	numPaymentOpsIngested := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "num_payment_ops_ingested",
		Help: "Number of payment operations ingested",
	}, []string{"operation_type"})
	numTssTransactionsIngested := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "num_tss_transactions_ingested",
		Help: "Number of tss transactions ingested",
	}, []string{"status"})
	paymentAmountsIngested := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "payment_amount_ingested",
		Help: "Amount of payment ingested",
	}, []string{"operation_type", "asset_type"})

	ingestService := &ingestService{
		models:           models,
		ledgerCursorName: ledgerCursorName,
		appTracker:       appTracker,
		rpcService:       rpcService,
		tssRouter:        tssRouter,
		tssStore:         tssStore,

		// Metrics
		numPaymentOpsIngested: numPaymentOpsIngested,
		numTssTransactionsIngested: numTssTransactionsIngested,
		paymentAmountsIngested: paymentAmountsIngested,
	}

	ingestService.RegisterMetrics(metricsRegistry)
	return ingestService, nil
}

func (m *ingestService) RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(
		m.numPaymentOpsIngested,
		m.numTssTransactionsIngested,
		m.paymentAmountsIngested,
	)
}

func (m *ingestService) Run(ctx context.Context, startLedger uint32, endLedger uint32) error {
	ingestHeartbeatChannel := make(chan any, 1)
	rpcHeartbeatChannel := m.rpcService.GetHeartbeatChannel()
	go trackIngestServiceHealth(ctx, ingestHeartbeatChannel, m.appTracker)

	if startLedger == 0 {
		var err error
		startLedger, err = m.models.Payments.GetLatestLedgerSynced(ctx, m.ledgerCursorName)
		if err != nil {
			return fmt.Errorf("erorr getting start ledger: %w", err)
		}
	}

	ingestLedger := startLedger
	for endLedger == 0 || ingestLedger <= endLedger {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case resp := <-rpcHeartbeatChannel:
			switch {
			// Case-1: wallet-backend is running behind rpc's oldest ledger. In this case, we start
			// ingestion from rpc's oldest ledger.
			case ingestLedger < resp.OldestLedger:
				ingestLedger = resp.OldestLedger
			// Case-2: rpc is running behind wallet-backend's latest synced ledger. We wait for rpc to
			// catch back up to wallet-backend.
			case ingestLedger > resp.LatestLedger:
				log.Debugf("waiting for RPC to catchup to ledger %d (latest: %d)",
					ingestLedger, resp.LatestLedger)
				time.Sleep(5 * time.Second)
				continue
			}
			log.Ctx(ctx).Infof("ingesting ledger: %d", ingestLedger)

			ledgerTransactions, err := m.GetLedgerTransactions(int64(ingestLedger))
			if err != nil {
				log.Error("getTransactions: %w", err)
				continue
			}
			ingestHeartbeatChannel <- true
			err = m.ingestPayments(ctx, ledgerTransactions)
			if err != nil {
				return fmt.Errorf("error ingesting payments: %w", err)
			}

			err = m.processTSSTransactions(ctx, ledgerTransactions)
			if err != nil {
				return fmt.Errorf("error processing tss transactions: %w", err)
			}

			err = m.models.Payments.UpdateLatestLedgerSynced(ctx, m.ledgerCursorName, ingestLedger)
			if err != nil {
				return fmt.Errorf("error updating latest synced ledger: %w", err)
			}

			ingestLedger++
		}
	}
	return nil
}

func (m *ingestService) GetLedgerTransactions(ledger int64) ([]entities.Transaction, error) {

	var ledgerTransactions []entities.Transaction
	var cursor string
	lastLedgerSeen := ledger
	for lastLedgerSeen == ledger {
		getTxnsResp, err := m.rpcService.GetTransactions(ledger, cursor, 50)
		if err != nil {
			return []entities.Transaction{}, fmt.Errorf("getTransactions: %w", err)
		}
		cursor = getTxnsResp.Cursor
		for _, tx := range getTxnsResp.Transactions {
			if tx.Ledger == ledger {
				ledgerTransactions = append(ledgerTransactions, tx)
				lastLedgerSeen = tx.Ledger
			} else {
				lastLedgerSeen = tx.Ledger
				break
			}
		}
	}
	return ledgerTransactions, nil
}

func (m *ingestService) ingestPayments(ctx context.Context, ledgerTransactions []entities.Transaction) error {
	return db.RunInTransaction(ctx, m.models.Payments.DB, nil, func(dbTx db.Transaction) error {
		for _, tx := range ledgerTransactions {
			if tx.Status != entities.SuccessStatus {
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
			txResultXDR, err := tss.UnmarshallTransactionResultXDR(tx.ResultXDR)
			if err != nil {
				return fmt.Errorf("cannot unmarshal transacation result xdr: %s", err.Error())
			}

			txMemo, txMemoType := utils.Memo(txEnvelopeXDR.Memo(), tx.Hash)
			if txMemo != nil {
				*txMemo = utils.SanitizeUTF8(*txMemo)
			}
			for idx, op := range txEnvelopeXDR.Operations() {
				opIdx := idx + 1
				payment := data.Payment{
					OperationID:     utils.OperationID(int32(tx.Ledger), int32(tx.ApplicationOrder), int32(opIdx)),
					OperationType:   op.Body.Type.String(),
					TransactionID:   utils.TransactionID(int32(tx.Ledger), int32(tx.ApplicationOrder)),
					TransactionHash: tx.Hash,
					FromAddress:     utils.SourceAccount(op, txEnvelopeXDR),
					CreatedAt:       time.Unix(int64(tx.CreatedAt), 0),
					Memo:            txMemo,
					MemoType:        txMemoType,
				}

				switch op.Body.Type {
				case xdr.OperationTypePayment:
					m.numPaymentOpsIngested.WithLabelValues(paymentPrometheusLabel).Inc()
					fillPayment(&payment, op.Body)
				case xdr.OperationTypePathPaymentStrictSend:
					m.numPaymentOpsIngested.WithLabelValues(pathPaymentStrictSendPrometheusLabel).Inc()
					fillPathSend(&payment, op.Body, txResultXDR, opIdx)
				case xdr.OperationTypePathPaymentStrictReceive:
					m.numPaymentOpsIngested.WithLabelValues(pathPaymentStrictReceivePrometheusLabel).Inc()
					fillPathReceive(&payment, op.Body, txResultXDR, opIdx)
				default:
					continue
				}

				err = m.models.Payments.AddPayment(ctx, dbTx, payment)
				if err != nil {
					return fmt.Errorf("adding payment for ledger %d, tx %s (%d), operation %s (%d): %w", tx.Ledger, tx.Hash, tx.ApplicationOrder, payment.OperationID, opIdx, err)
				}
				m.paymentAmountsIngested.WithLabelValues(payment.OperationType, payment.SrcAssetType).Set(float64(payment.SrcAmount))
			}
		}
		return nil
	})
}

func (m *ingestService) processTSSTransactions(ctx context.Context, ledgerTransactions []entities.Transaction) error {
	for _, tx := range ledgerTransactions {
		if !tx.FeeBump {
			// because all transactions submitted by TSS are fee bump transactions
			continue
		}
		tssTry, err := m.tssStore.GetTry(ctx, tx.Hash)
		if err != nil {
			return fmt.Errorf("error when getting try: %w", err)
		}
		if tssTry == (tssstore.Try{}) {
			continue
		}

		transaction, err := m.tssStore.GetTransaction(ctx, tssTry.OrigTxHash)
		if err != nil {
			return fmt.Errorf("error getting transaction: %w", err)
		}
		status := tss.RPCTXStatus{RPCStatus: tx.Status}
		code, err := tss.TransactionResultXDRToCode(tx.ResultXDR)
		if err != nil {
			return fmt.Errorf("error unmarshaling resultxdr: %w", err)
		}
		err = m.tssStore.UpsertTry(ctx, tssTry.OrigTxHash, tssTry.Hash, tssTry.XDR, status, code, tx.ResultXDR)
		if err != nil {
			return fmt.Errorf("error updating try: %w", err)
		}
		err = m.tssStore.UpsertTransaction(ctx, transaction.WebhookURL, tssTry.OrigTxHash, transaction.XDR, status)
		if err != nil {
			return fmt.Errorf("error updating transaction: %w", err)
		}

		txCode, err := tss.TransactionResultXDRToCode(tx.ResultXDR)
		if err != nil {
			return fmt.Errorf("unable to extract tx code from result xdr string: %w", err)
		}

		tssGetIngestResponse := tss.RPCGetIngestTxResponse{
			Status:      tx.Status,
			Code:        txCode,
			EnvelopeXDR: tx.EnvelopeXDR,
			ResultXDR:   tx.ResultXDR,
			CreatedAt:   int64(tx.CreatedAt),
		}
		payload := tss.Payload{
			RpcGetIngestTxResponse: tssGetIngestResponse,
		}
		err = m.tssRouter.Route(payload)
		if err != nil {
			return fmt.Errorf("unable to route payload: %w", err)
		}
		m.numTssTransactionsIngested.WithLabelValues(status.Status()).Inc()
	}
	return nil
}

func trackIngestServiceHealth(ctx context.Context, heartbeat chan any, tracker apptracker.AppTracker) {
	ticker := time.NewTicker(ingestHealthCheckMaxWaitTime)
	defer func() {
		ticker.Stop()
		close(heartbeat)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			warn := fmt.Sprintf("ingestion service stale for over %s", ingestHealthCheckMaxWaitTime)
			log.Warn(warn)
			if tracker != nil {
				tracker.CaptureMessage(warn)
			} else {
				log.Warn("App Tracker is nil")
			}
			ticker.Reset(ingestHealthCheckMaxWaitTime)
		case <-heartbeat:
			ticker.Reset(ingestHealthCheckMaxWaitTime)
		}
	}
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

func fillPathSend(payment *data.Payment, operation xdr.OperationBody, txResult xdr.TransactionResult, operationIdx int) {
	pathOp := operation.MustPathPaymentStrictSendOp()
	result := utils.OperationResult(txResult, operationIdx).MustPathPaymentStrictSendResult()
	payment.ToAddress = pathOp.Destination.Address()
	payment.SrcAssetCode = utils.AssetCode(pathOp.SendAsset)
	payment.SrcAssetIssuer = pathOp.SendAsset.GetIssuer()
	payment.SrcAssetType = pathOp.SendAsset.Type.String()
	payment.SrcAmount = int64(pathOp.SendAmount)
	payment.DestAssetCode = utils.AssetCode(pathOp.DestAsset)
	payment.DestAssetIssuer = pathOp.DestAsset.GetIssuer()
	payment.DestAssetType = pathOp.DestAsset.Type.String()
	if result != (xdr.PathPaymentStrictSendResult{}) {
		payment.DestAmount = int64(result.DestAmount())
	}
}

func fillPathReceive(payment *data.Payment, operation xdr.OperationBody, txResult xdr.TransactionResult, operationIdx int) {
	pathOp := operation.MustPathPaymentStrictReceiveOp()
	result := utils.OperationResult(txResult, operationIdx).MustPathPaymentStrictReceiveResult()
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
