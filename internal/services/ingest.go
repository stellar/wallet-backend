package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alitto/pond"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-rpc/protocol"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/signing/store"
	txutils "github.com/stellar/wallet-backend/internal/transactions/utils"
	"github.com/stellar/wallet-backend/internal/utils"
)

var ErrAlreadyInSync = errors.New("ingestion is already in sync")

const advisoryLockID = int(3747555612780983)

const (
	ingestHealthCheckMaxWaitTime            = 90 * time.Second
	paymentPrometheusLabel                  = "payment"
	pathPaymentStrictSendPrometheusLabel    = "path_payment_strict_send"
	pathPaymentStrictReceivePrometheusLabel = "path_payment_strict_receive"
	totalIngestionPrometheusLabel           = "total"
)

type IngestService interface {
	Run(ctx context.Context, startLedger uint32, endLedger uint32) error
}

var _ IngestService = (*ingestService)(nil)

type ingestService struct {
	models            *data.Models
	ledgerCursorName  string
	appTracker        apptracker.AppTracker
	rpcService        RPCService
	chAccStore        store.ChannelAccountStore
	metricsService    metrics.MetricsService
	networkPassphrase string
}

func NewIngestService(
	models *data.Models,
	ledgerCursorName string,
	appTracker apptracker.AppTracker,
	rpcService RPCService,
	chAccStore store.ChannelAccountStore,
	metricsService metrics.MetricsService,
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
	if chAccStore == nil {
		return nil, errors.New("chAccStore cannot be nil")
	}
	if metricsService == nil {
		return nil, errors.New("metricsService cannot be nil")
	}

	return &ingestService{
		models:            models,
		ledgerCursorName:  ledgerCursorName,
		appTracker:        appTracker,
		rpcService:        rpcService,
		chAccStore:        chAccStore,
		metricsService:    metricsService,
		networkPassphrase: rpcService.NetworkPassphrase(),
	}, nil
}

const maxLedgerWindow = 200 // NOTE: cannot be larger than 200

// getLedgerSeqRange returns a ledger sequence range to ingest. It takes into account:
// - the ledgers available in the RPC,
// - the latest ledger synced by the ingestion service,
// - the max ledger window to ingest.
//
// The returned ledger sequence range is inclusive of the start and end ledgers.
func getLedgerSeqRange(rpcOldestLedger, rpcNewestLedger, latestLedgerSynced uint32) (ledgerRange LedgerSeqRange, inSync bool) {
	if latestLedgerSynced >= rpcNewestLedger {
		return LedgerSeqRange{}, true
	}

	ledgerRange.Start = max(latestLedgerSynced+1, rpcOldestLedger)
	ledgerRange.End = min(ledgerRange.Start+(maxLedgerWindow-1), rpcNewestLedger)

	return ledgerRange, false
}

func (m *ingestService) Run(ctx context.Context, startLedger uint32, endLedger uint32) error {
	// Acquire advisory lock to prevent multiple ingestion instances from running concurrently
	if lockAcquired, err := db.AcquireAdvisoryLock(ctx, m.models.DB, advisoryLockID); err != nil {
		return fmt.Errorf("acquiring advisory lock: %w", err)
	} else if !lockAcquired {
		return errors.New("advisory lock not acquired")
	}
	defer func() {
		if err := db.ReleaseAdvisoryLock(ctx, m.models.DB, advisoryLockID); err != nil {
			err = fmt.Errorf("releasing advisory lock: %w", err)
			log.Ctx(ctx).Error(err)
		}
	}()

	// get latest ledger synced, to use as a cursor
	if startLedger == 0 {
		var err error
		startLedger, err = m.models.IngestStore.GetLatestLedgerSynced(ctx, m.ledgerCursorName)
		if err != nil {
			return fmt.Errorf("getting latest ledger synced: %w", err)
		}
	}

	const tickerDuration = time.Second * 5
	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	defer signal.Stop(signalChan)
	manualTriggerChan := make(chan any, 1)
	defer close(manualTriggerChan)

	log.Ctx(ctx).Info("Starting ingestion loop")
	for {
		select {
		case sig := <-signalChan:
			log.Ctx(ctx).Info("Signal received")
			return fmt.Errorf("received signal %s while waiting for RPC service to become healthy", sig)
		case <-ctx.Done():
			log.Ctx(ctx).Info("Context cancelled")
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case <-manualTriggerChan:
			log.Ctx(ctx).Info("Manual trigger received")
		case <-ticker.C:
			log.Ctx(ctx).Info("Ticker ticked")
		}

		totalIngestionStart := time.Now()
		// fetch ledgers
		getLedgersResponse, err := m.fetchNextLedgersBatch(ctx, startLedger)
		if err != nil {
			if errors.Is(err, ErrAlreadyInSync) {
				log.Ctx(ctx).Info("Ingestion is already in sync, will retry in a few moments...")
				continue
			}
			return fmt.Errorf("fetching next ledgers batch: %w", err)
		}

		// process ledgers
		err = m.processLedgerResponse(ctx, getLedgersResponse)
		if err != nil {
			return fmt.Errorf("processing ledger response: %w", err)
		}

		// update cursor
		startLedger = getLedgersResponse.Ledgers[len(getLedgersResponse.Ledgers)-1].Sequence
		err = m.models.IngestStore.UpdateLatestLedgerSynced(ctx, m.ledgerCursorName, startLedger)
		if err != nil {
			return fmt.Errorf("updating latest synced ledger: %w", err)
		}
		m.metricsService.SetLatestLedgerIngested(float64(getLedgersResponse.LatestLedger))
		m.metricsService.ObserveIngestionDuration(totalIngestionPrometheusLabel, time.Since(totalIngestionStart).Seconds())

		ticker.Reset(tickerDuration)

		if len(getLedgersResponse.Ledgers) > maxLedgerWindow {
			manualTriggerChan <- nil
		}
	}
}

type jobResultData struct {
	ledger        protocol.LedgerInfo
	processedData indexer.DataBundle
	err           error
}

func (m *ingestService) processLedgerResponse(ctx context.Context, getLedgersResponse GetLedgersResponse) error {
	log.Ctx(ctx).Infof("🚧 Will process & ingest %d ledgers", len(getLedgersResponse.Ledgers))

	// Create a worker pool with
	const poolSize = 16
	pool := pond.New(poolSize, maxLedgerWindow, pond.Context(ctx))

	// Create a slice to store jobResults
	var errs []error
	dataProcessor := indexer.NewParticipantsProcessor(m.networkPassphrase)

	// Submit tasks to the pool
	for _, ledger := range getLedgersResponse.Ledgers {
		ledger := ledger // Create a new variable to avoid closure issues
		pool.Submit(func() {
			if err := m.processLedger(ctx, ledger, dataProcessor); err != nil {
				errs = append(errs, fmt.Errorf("processing ledger %d: %w", ledger.Sequence, err))
			}
		})
	}

	// Wait for all tasks to complete
	pool.StopAndWait()

	if len(errs) > 0 {
		return fmt.Errorf("processing ledgers: %w", errors.Join(errs...))
	}

	err := m.ingestProcessedData(ctx, dataProcessor)
	if err != nil {
		return fmt.Errorf("ingesting processed data: %w", err)
	}

	log.Ctx(ctx).Infof("🚧 Done processing & ingesting %d ledgers", len(getLedgersResponse.Ledgers))

	return nil
}

func (m *ingestService) ingestProcessedData(ctx context.Context, dataProcessor *indexer.ParticipantsProcessor) error {
	return db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		dataBundle := &dataProcessor.DataBundle
		// 1. Filter participants that are not in the watchlist.
		existingAccounts, err := m.models.Account.GetExisting(ctx, dbTx, dataBundle.Participants.ToSlice())
		if err != nil {
			return fmt.Errorf("getting existing accounts: %w", err)
		}

		if len(existingAccounts) == 0 {
			return nil
		}

		// 2. Identify which data should be ingested.
		txHashesToInsert := make(indexer.Set[string])
		participantsByTxHash := map[string][]string{}
		for _, participant := range existingAccounts {
			if !dataBundle.Participants.Contains(participant) {
				continue
			}

			// 2.1. Identify which transactions should be ingested.
			participantTxHashes := dataBundle.GetParticipantTransactionHashes(participant)
			txHashesToInsert.Append(participantTxHashes)
			for txHash := range participantTxHashes {
				participantsByTxHash[txHash] = append(participantsByTxHash[txHash], participant)
			}

			// 2.2. TODO: Identify which operations should be ingested.
		}

		// 3. Build the data to be ingested.
		var transactionsToInsert = make([]types.Transaction, 0, len(txHashesToInsert))
		for txHash := range txHashesToInsert {
			transactionsToInsert = append(transactionsToInsert, dataBundle.TxByHash[txHash])
		}

		// 4. Insert the transactions into the database.
		log.Ctx(ctx).Infof("inserting %d transactions with participants %#v", len(transactionsToInsert), participantsByTxHash)
		err = m.models.Transactions.BatchInsert(ctx, dbTx, transactionsToInsert, participantsByTxHash)
		if err != nil {
			return fmt.Errorf("inserting transactions: %w", err)
		}

		// 5. Unlock channel accounts.
		txEnvelopeXDRs := make([]string, 0, len(dataBundle.TxByHash))
		for _, tx := range dataBundle.TxByHash {
			txEnvelopeXDRs = append(txEnvelopeXDRs, tx.EnvelopeXDR)
		}
		err = m.unlockChannelAccounts(ctx, dbTx, txEnvelopeXDRs)
		if err != nil {
			return fmt.Errorf("unlocking channel accounts: %w", err)
		}

		return nil
	})
}

func (m *ingestService) processLedger(ctx context.Context, ledgerInfo protocol.LedgerInfo, dataProcessor *indexer.ParticipantsProcessor) error {
	var xdrLedgerCloseMeta xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshalBase64(ledgerInfo.LedgerMetadata, &xdrLedgerCloseMeta); err != nil {
		return fmt.Errorf("unmarshalling ledger close meta: %w", err)
	}

	transactions, err := m.getLedgerTransactions(ctx, xdrLedgerCloseMeta)
	if err != nil {
		return fmt.Errorf("getting ledger transactions: %w", err)
	}

	for _, tx := range transactions {
		err := dataProcessor.ProcessTransactionData(tx)
		if err != nil {
			return fmt.Errorf("processing transaction: %w", err)
		}
	}

	return nil
}

func (m *ingestService) getLedgerTransactions(_ context.Context, xdrLedgerCloseMeta xdr.LedgerCloseMeta) ([]ingest.LedgerTransaction, error) {
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(m.networkPassphrase, xdrLedgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("creating ledger transaction reader: %w", err)
	}
	defer ledgerTxReader.Close()

	transactions := make([]ingest.LedgerTransaction, 0)
	for {
		tx, err := ledgerTxReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("reading ledger: %w", err)
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// fetchNextLedgersBatch fetches the next batch of ledgers from the RPC service.
func (m *ingestService) fetchNextLedgersBatch(ctx context.Context, startLedger uint32) (GetLedgersResponse, error) {
	rpcHealth, err := m.rpcService.GetHealth()
	if err != nil {
		return GetLedgersResponse{}, fmt.Errorf("getting rpc health: %w", err)
	}
	ledgerSeqRange, inSync := getLedgerSeqRange(rpcHealth.OldestLedger, rpcHealth.LatestLedger, startLedger)
	log.Ctx(ctx).Debugf("ledgerSeqRange: %v", ledgerSeqRange)
	if inSync {
		return GetLedgersResponse{}, ErrAlreadyInSync
	}

	getLedgersResponse, err := m.rpcService.GetLedgers(ledgerSeqRange)
	if err != nil {
		return GetLedgersResponse{}, fmt.Errorf("getting ledgers: %w", err)
	}

	return getLedgersResponse, nil
}

// RunOld is the old ingestion loop. It is being deprecated.
func (m *ingestService) RunOld(ctx context.Context, startLedger uint32, endLedger uint32) error {
	manualTriggerChannel := make(chan any, 1)
	go m.rpcService.TrackRPCServiceHealth(ctx, manualTriggerChannel)
	ingestHeartbeatChannel := make(chan any, 1)
	rpcHeartbeatChannel := m.rpcService.GetHeartbeatChannel()
	go trackIngestServiceHealth(ctx, ingestHeartbeatChannel, m.appTracker)

	if startLedger == 0 {
		var err error
		startLedger, err = m.models.IngestStore.GetLatestLedgerSynced(ctx, m.ledgerCursorName)
		if err != nil {
			return fmt.Errorf("getting latest ledger synced: %w", err)
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
			log.Ctx(ctx).Infof("ingesting ledger: %d, oldest: %d, latest: %d", ingestLedger, resp.OldestLedger, resp.LatestLedger)

			start := time.Now()
			ledgerTransactions, err := m.GetLedgerTransactions(int64(ingestLedger))
			if err != nil {
				log.Error("getTransactions: %w", err)
				continue
			}
			ingestHeartbeatChannel <- true
			startTime := time.Now()
			err = m.ingestPayments(ctx, ledgerTransactions)
			if err != nil {
				return fmt.Errorf("ingesting payments: %w", err)
			}
			m.metricsService.ObserveIngestionDuration(paymentPrometheusLabel, time.Since(startTime).Seconds())

			// eagerly unlock channel accounts from txs
			err = m.unlockChannelAccountsOld(ctx, ledgerTransactions)
			if err != nil {
				return fmt.Errorf("unlocking channel account from tx: %w", err)
			}

			// update cursor
			err = m.models.IngestStore.UpdateLatestLedgerSynced(ctx, m.ledgerCursorName, ingestLedger)
			if err != nil {
				return fmt.Errorf("updating latest synced ledger: %w", err)
			}
			m.metricsService.SetLatestLedgerIngested(float64(ingestLedger))
			m.metricsService.ObserveIngestionDuration(totalIngestionPrometheusLabel, time.Since(start).Seconds())

			// immediately trigger the next ingestion the wallet-backend is behind the RPC's latest ledger
			if resp.LatestLedger-ingestLedger > 1 {
				manualTriggerChannel <- true
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
	err := db.RunInTransaction(ctx, m.models.DB, nil, func(dbTx db.Transaction) error {
		paymentOpsIngested := 0
		pathPaymentStrictSendOpsIngested := 0
		pathPaymentStrictReceiveOpsIngested := 0
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
			txResultXDR, err := txutils.UnmarshallTransactionResultXDR(tx.ResultXDR)
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
					paymentOpsIngested++
					fillPayment(&payment, op.Body)
				case xdr.OperationTypePathPaymentStrictSend:
					pathPaymentStrictSendOpsIngested++
					fillPathSend(&payment, op.Body, txResultXDR, opIdx)
				case xdr.OperationTypePathPaymentStrictReceive:
					pathPaymentStrictReceiveOpsIngested++
					fillPathReceive(&payment, op.Body, txResultXDR, opIdx)
				default:
					continue
				}

				err = m.models.Payments.AddPayment(ctx, dbTx, payment)
				if err != nil {
					return fmt.Errorf("adding payment for ledger %d, tx %s (%d), operation %s (%d): %w", tx.Ledger, tx.Hash, tx.ApplicationOrder, payment.OperationID, opIdx, err)
				}
			}
		}
		m.metricsService.SetNumPaymentOpsIngestedPerLedger(paymentPrometheusLabel, paymentOpsIngested)
		m.metricsService.SetNumPaymentOpsIngestedPerLedger(pathPaymentStrictSendPrometheusLabel, pathPaymentStrictSendOpsIngested)
		m.metricsService.SetNumPaymentOpsIngestedPerLedger(pathPaymentStrictReceivePrometheusLabel, pathPaymentStrictReceiveOpsIngested)
		return nil
	})
	if err != nil {
		return fmt.Errorf("ingesting payments: %w", err)
	}

	return nil
}

// unlockChannelAccounts unlocks the channel accounts associated with the given transaction XDRs.
func (m *ingestService) unlockChannelAccounts(ctx context.Context, dbTx db.Transaction, txEnvelopeXDRs []string) error {
	if len(txEnvelopeXDRs) == 0 {
		log.Ctx(ctx).Debug("no transactions to unlock channel accounts from")
		return nil
	}

	innerTxHashes := make([]string, 0, len(txEnvelopeXDRs))
	for _, txEnvelopeXDR := range txEnvelopeXDRs {
		if innerTxHash, err := m.extractInnerTxHash(txEnvelopeXDR); err != nil {
			return fmt.Errorf("extracting inner tx hash: %w", err)
		} else {
			innerTxHashes = append(innerTxHashes, innerTxHash)
		}
	}

	if affectedRows, err := m.chAccStore.UnassignTxAndUnlockChannelAccounts(ctx, dbTx, innerTxHashes...); err != nil {
		return fmt.Errorf("unlocking channel accounts with txHashes %v: %w", innerTxHashes, err)
	} else if affectedRows > 0 {
		log.Ctx(ctx).Infof("unlocked %d channel accounts", affectedRows)
	}

	return nil
}

// unlockChannelAccountsOld unlocks the channel accounts associated with the given transaction XDRs.
func (m *ingestService) unlockChannelAccountsOld(ctx context.Context, ledgerTransactions []entities.Transaction) error {
	if len(ledgerTransactions) == 0 {
		log.Ctx(ctx).Debug("no transactions to unlock channel accounts from")
		return nil
	}

	innerTxHashes := make([]string, 0, len(ledgerTransactions))
	for _, tx := range ledgerTransactions {
		if innerTxHash, err := m.extractInnerTxHash(tx.EnvelopeXDR); err != nil {
			return fmt.Errorf("extracting inner tx hash: %w", err)
		} else {
			innerTxHashes = append(innerTxHashes, innerTxHash)
		}
	}

	if affectedRows, err := m.chAccStore.UnassignTxAndUnlockChannelAccounts(ctx, nil, innerTxHashes...); err != nil {
		return fmt.Errorf("unlocking channel accounts with txHashes %v: %w", innerTxHashes, err)
	} else if affectedRows > 0 {
		log.Ctx(ctx).Infof("unlocked %d channel accounts", affectedRows)
	}

	return nil
}

// extractInnerTxHash takes a transaction XDR string and returns the hash of its inner transaction.
// For fee bump transactions, it returns the hash of the inner transaction.
// For regular transactions, it returns the hash of the transaction itself.
func (m *ingestService) extractInnerTxHash(txXDR string) (string, error) {
	genericTx, err := txnbuild.TransactionFromXDR(txXDR)
	if err != nil {
		return "", fmt.Errorf("deserializing envelope xdr %q: %w", txXDR, err)
	}

	var innerTx *txnbuild.Transaction
	feeBumpTx, ok := genericTx.FeeBump()
	if ok {
		innerTx = feeBumpTx.InnerTransaction()
	} else {
		innerTx, ok = genericTx.Transaction()
		if !ok {
			return "", errors.New("transaction is neither fee bump nor inner transaction")
		}
	}

	innerTxHash, err := innerTx.HashHex(m.networkPassphrase)
	if err != nil {
		return "", fmt.Errorf("generating hash hex: %w", err)
	}

	return innerTxHash, nil
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
