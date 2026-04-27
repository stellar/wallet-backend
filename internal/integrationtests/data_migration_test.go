package integrationtests

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	_ "github.com/stellar/wallet-backend/internal/services/sep41" // registers SEP-41 validator + processor via init()
)

const sep41ProtocolID = "SEP41"

type DataMigrationTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (s *DataMigrationTestSuite) SetupTest() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	s.resetDataMigrationState(ctx, pool)
}

func (s *DataMigrationTestSuite) setupDB() (*pgxpool.Pool, func()) {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)

	pool, err := db.OpenDBConnectionPool(ctx, dbURL)
	s.Require().NoError(err)

	return pool, func() { pool.Close() }
}

func (s *DataMigrationTestSuite) setupModels(pool *pgxpool.Pool) *data.Models {
	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	models, err := data.NewModels(pool, dbMetrics)
	s.Require().NoError(err)

	return models
}

func (s *DataMigrationTestSuite) resetDataMigrationState(ctx context.Context, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, `UPDATE protocol_wasms SET protocol_id = NULL`)
	s.Require().NoError(err)

	_, err = pool.Exec(ctx, `DELETE FROM protocols`)
	s.Require().NoError(err)

	_, err = pool.Exec(ctx, `
DELETE FROM ingest_store
WHERE key LIKE 'test_%'
   OR key LIKE 'protocol_SEP41_%'
   OR key LIKE 'protocol_testproto%'
`)
	s.Require().NoError(err)
}

func (s *DataMigrationTestSuite) upsertIngestStoreValue(ctx context.Context, pool *pgxpool.Pool, key string, value uint32) {
	_, err := pool.Exec(ctx,
		`INSERT INTO ingest_store (key, value) VALUES ($1, $2)
 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		key, strconv.FormatUint(uint64(value), 10))
	s.Require().NoError(err)
}

func (s *DataMigrationTestSuite) ingestStoreKeyExists(ctx context.Context, pool *pgxpool.Pool, key string) bool {
	var exists bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ingest_store WHERE key = $1)`, key).Scan(&exists)
	s.Require().NoError(err)
	return exists
}

func (s *DataMigrationTestSuite) runSEP41ProtocolSetup(ctx context.Context, pool *pgxpool.Pool, models *data.Models) {
	err := db.RunInTransaction(ctx, pool, func(dbTx pgx.Tx) error {
		return models.Protocols.InsertIfNotExists(ctx, dbTx, sep41ProtocolID)
	})
	s.Require().NoError(err)

	specExtractor := services.NewWasmSpecExtractor()

	metadataPool := pond.NewPool(0)
	defer metadataPool.StopAndWait()
	metadataService, mErr := services.NewContractMetadataService(s.testEnv.RPCService, models.Contract, metadataPool)
	s.Require().NoError(mErr)

	deps := services.ProtocolDeps{
		NetworkPassphrase:       s.testEnv.NetworkPassphrase,
		Models:                  models,
		RPCService:              s.testEnv.RPCService,
		ContractMetadataService: metadataService,
	}
	validators, cErr := services.BuildValidators(deps, []string{sep41ProtocolID})
	s.Require().NoError(cErr)

	svc := services.NewProtocolSetupService(
		pool,
		s.testEnv.RPCService,
		models,
		specExtractor,
		validators,
	)

	s.Require().NoError(svc.Run(ctx, []string{sep41ProtocolID}))
}

func (s *DataMigrationTestSuite) newTokenIngestionServiceMock() *services.TokenIngestionServiceMock {
	mockTokenIngestionService := services.NewTokenIngestionServiceMock(s.T())
	mockTokenIngestionService.On(
		"ProcessTokenChanges",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil).Maybe()
	return mockTokenIngestionService
}

const protocolStateProductionLedgerMetaWith0Tx = "AAAAAQAAAACB7Zh2o0NTFwl1nvs7xr3SJ7w8PpwnSRb8QyG9k6acEwAAABaeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLYjyoO5BI41g1PFT+iHW68giP49Koo+q3VmH8I4GdtW2AAAAAGhTTB8AAAAAAAAAAQAAAAC1XRCyu30oTtXAOkel4bWQyQ9Xg1VHHMRQe76CBNI8iwAAAEDSH4sE7cL7UJyOqUo9ZZeNqPT7pt7su8iijHjWYg4MbeFUh/gkGf6N40bZjP/dlIuGXmuEhWoEX0VTV58xOB4C3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERm+pITz+1V1m+3/v6eaEKglCnon3a5xkn02sLltJ9CSzwAAEYIN4Lazp2QAAAAAAAMtYtQzAAAAAAAAAAAAAAAMAAAAZABMS0AAAADIXukLfWC53MCmzxKd/+LBbaYxQkgxATFDLI3hWj7EqWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGeASPlzu/ZFxwyyWsxtGoj3KCrybm2yN7WOweR0BWdLQAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9yHMAAAAAAAAAAA=="

type singleLedgerBackend struct {
	ledgerSeq  uint32
	ledgerMeta xdr.LedgerCloseMeta
}

func (b *singleLedgerBackend) GetLatestLedgerSequence(context.Context) (uint32, error) {
	return b.ledgerSeq, nil
}

func (b *singleLedgerBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	if sequence == b.ledgerSeq {
		return b.ledgerMeta, nil
	}

	<-ctx.Done()
	return xdr.LedgerCloseMeta{}, ctx.Err()
}

func (b *singleLedgerBackend) PrepareRange(context.Context, ledgerbackend.Range) error {
	return nil
}

func (b *singleLedgerBackend) IsPrepared(context.Context, ledgerbackend.Range) (bool, error) {
	return true, nil
}

func (b *singleLedgerBackend) Close() error {
	return nil
}

type rangeLedgerBackend struct {
	startSeq, endSeq uint32
	ledgerMeta       xdr.LedgerCloseMeta
	onMiss           func(seq uint32)
	onMissOnce       sync.Once
}

func (b *rangeLedgerBackend) GetLatestLedgerSequence(context.Context) (uint32, error) {
	return b.endSeq, nil
}

func (b *rangeLedgerBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	if sequence >= b.startSeq && sequence <= b.endSeq {
		return b.ledgerMeta, nil
	}
	if b.onMiss != nil {
		var called bool
		b.onMissOnce.Do(func() {
			b.onMiss(sequence)
			called = true
		})
		if called {
			return b.ledgerMeta, nil
		}
	}
	<-ctx.Done()
	return xdr.LedgerCloseMeta{}, ctx.Err()
}

func (b *rangeLedgerBackend) PrepareRange(context.Context, ledgerbackend.Range) error {
	return nil
}

func (b *rangeLedgerBackend) IsPrepared(context.Context, ledgerbackend.Range) (bool, error) {
	return true, nil
}

func (b *rangeLedgerBackend) Close() error {
	return nil
}

type integrationTestProcessor struct {
	id                        string
	processedLedger           uint32
	seenContracts             []data.ProtocolContracts
	ingestStore               *data.IngestStoreModel
	persistedHistorySeqs      []uint32
	persistedCurrentStateSeqs []uint32
}

func (p *integrationTestProcessor) ProtocolID() string { return p.id }

func (p *integrationTestProcessor) ProcessLedger(_ context.Context, input services.ProtocolProcessorInput) error {
	p.processedLedger = input.LedgerSequence
	p.seenContracts = append([]data.ProtocolContracts(nil), input.ProtocolContracts...)
	return nil
}

func (p *integrationTestProcessor) PersistHistory(ctx context.Context, dbTx pgx.Tx) error {
	p.persistedHistorySeqs = append(p.persistedHistorySeqs, p.processedLedger)
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_history_written", p.id), p.processedLedger)
}

func (p *integrationTestProcessor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	p.persistedCurrentStateSeqs = append(p.persistedCurrentStateSeqs, p.processedLedger)
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_current_state_written", p.id), p.processedLedger)
}

func (p *integrationTestProcessor) LoadCurrentState(_ context.Context, _ pgx.Tx) error {
	return nil
}

func (s *DataMigrationTestSuite) mustLedgerCloseMeta() xdr.LedgerCloseMeta {
	var ledgerMeta xdr.LedgerCloseMeta
	err := xdr.SafeUnmarshalBase64(protocolStateProductionLedgerMetaWith0Tx, &ledgerMeta)
	s.Require().NoError(err)
	return ledgerMeta
}

func (s *DataMigrationTestSuite) newLiveRunService(
	models *data.Models,
	rpcService services.RPCService,
	ledgerBackend ledgerbackend.LedgerBackend,
	m *metrics.Metrics,
	processor services.ProtocolProcessor,
) services.IngestService {
	svc, err := services.NewIngestService(services.IngestServiceConfig{
		IngestionMode:          services.IngestionModeLive,
		Models:                 models,
		OldestLedgerCursorName: data.OldestLedgerCursorName,
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             rpcService,
		LedgerBackend:          ledgerBackend,
		TokenIngestionService:  s.newTokenIngestionServiceMock(),
		Metrics:                m,
		Network:                "protocol-state-production-live-run-test",
		NetworkPassphrase:      "Test SDF Network ; September 2015",
		ProtocolProcessors:     []services.ProtocolProcessor{processor},
	})
	s.Require().NoError(err)
	return svc
}

func (s *DataMigrationTestSuite) newHistoryMigrationService(
	pool *pgxpool.Pool,
	models *data.Models,
	ledgerBackend ledgerbackend.LedgerBackend,
	processor services.ProtocolProcessor,
) services.ProtocolMigrateHistoryService {
	svc, err := services.NewProtocolMigrateHistoryService(services.ProtocolMigrateHistoryConfig{
		DB:                     pool,
		LedgerBackend:          ledgerBackend,
		ProtocolsModel:         models.Protocols,
		ProtocolContractsModel: models.ProtocolContracts,
		IngestStore:            models.IngestStore,
		NetworkPassphrase:      "Test SDF Network ; September 2015",
		Processors:             []services.ProtocolProcessor{processor},
		OldestLedgerCursorName: data.OldestLedgerCursorName,
	})
	s.Require().NoError(err)
	return svc
}

func protocolContractKeys(contracts []data.ProtocolContracts) []string {
	keys := make([]string, len(contracts))
	for i, contract := range contracts {
		keys[i] = contract.ContractID.String() + "|" + contract.WasmHash.String()
	}
	sort.Strings(keys)
	return keys
}

func (s *DataMigrationTestSuite) TestProtocolSetupClassifiesIngestedWasms() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	models := s.setupModels(pool)

	unclassifiedBefore, err := models.ProtocolWasms.GetUnclassified(ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(unclassifiedBefore, "ingest should have populated protocol_wasms with unclassified entries")
	totalWasms := len(unclassifiedBefore)
	s.T().Logf("Found %d unclassified WASMs from ingest", totalWasms)

	s.runSEP41ProtocolSetup(ctx, pool, models)

	unclassifiedAfter, err := models.ProtocolWasms.GetUnclassified(ctx)
	s.Require().NoError(err)
	classifiedCount := totalWasms - len(unclassifiedAfter)
	s.Assert().Greater(classifiedCount, 0, "at least one WASM should have been classified as SEP41")
	s.T().Logf("Classified %d/%d WASMs as SEP41", classifiedCount, totalWasms)

	protocols, err := models.Protocols.GetByIDs(ctx, []string{sep41ProtocolID})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].ClassificationStatus)

	var cursorCount int
	err = pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM ingest_store WHERE key IN ($1, $2)`,
		"protocol_SEP41_history_cursor", "protocol_SEP41_current_state_cursor").Scan(&cursorCount)
	s.Require().NoError(err)
	s.Assert().Zero(cursorCount, "protocol-setup should not initialize protocol cursors")

	s.Assert().NotEmpty(unclassifiedAfter, "some WASMs (increment contract, SAC WASMs) should remain unclassified")
	s.T().Logf("%d WASMs remain unclassified", len(unclassifiedAfter))
}

func (s *DataMigrationTestSuite) TestLiveIngestionProcessesSetupClassifiedSEP41WhenCursorsReady() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	models := s.setupModels(pool)

	latestLedger, err := models.IngestStore.Get(ctx, "latest_ingest_ledger")
	s.Require().NoError(err)
	s.Require().Greater(latestLedger, uint32(0), "live ingest should have populated latest_ingest_ledger")
	testLedger := latestLedger + 1000

	s.runSEP41ProtocolSetup(ctx, pool, models)

	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, sep41ProtocolID)
	s.Require().NoError(err)
	s.Require().NotEmpty(classifiedContracts, "setup-classified SEP41 contracts should be queryable for live ingestion")
	expectedContractKeys := protocolContractKeys(classifiedContracts)

	const cursorName = data.LatestLedgerCursorName
	s.upsertIngestStoreValue(ctx, pool, cursorName, testLedger-1)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_history_cursor", testLedger-1)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_current_state_cursor", testLedger-1)

	processor := &integrationTestProcessor{id: sep41ProtocolID, ingestStore: models.IngestStore}
	rpcService := services.NewRPCServiceMock(s.T())
	rpcService.On("GetHealth").Return(entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: testLedger,
		OldestLedger: 1,
	}, nil).Once()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	ledgerBackend := &singleLedgerBackend{
		ledgerSeq:  testLedger,
		ledgerMeta: s.mustLedgerCloseMeta(),
	}
	svc := s.newLiveRunService(models, rpcService, ledgerBackend, m, processor)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- svc.Run(runCtx, 0, 0)
	}()

	var earlyRunErr error
	require.Eventually(s.T(), func() bool {
		select {
		case earlyRunErr = <-runErrCh:
			return true
		default:
		}

		cursor, err := models.IngestStore.Get(ctx, cursorName)
		if err != nil || cursor != testLedger {
			return false
		}

		historyCursor, err := models.IngestStore.Get(ctx, "protocol_SEP41_history_cursor")
		if err != nil || historyCursor != testLedger {
			return false
		}

		currentStateCursor, err := models.IngestStore.Get(ctx, "protocol_SEP41_current_state_cursor")
		if err != nil || currentStateCursor != testLedger {
			return false
		}

		historyWritten, err := models.IngestStore.Get(ctx, "test_SEP41_history_written")
		if err != nil || historyWritten != testLedger {
			return false
		}

		currentStateWritten, err := models.IngestStore.Get(ctx, "test_SEP41_current_state_written")
		return err == nil && currentStateWritten == testLedger
	}, 10*time.Second, 100*time.Millisecond)

	s.Require().NoError(earlyRunErr, "live Run exited before the expected DB state was committed")
	s.Assert().Equal(testLedger, processor.processedLedger, "ProcessLedger should run before history/current-state persistence")
	s.Assert().Equal(expectedContractKeys, protocolContractKeys(processor.seenContracts), "live ingestion should process the contracts classified during protocol setup")

	cancel()

	select {
	case err := <-runErrCh:
		s.Require().Error(err)
		s.Require().ErrorIs(err, context.Canceled)
	case <-time.After(5 * time.Second):
		s.FailNow("timed out waiting for live Run to stop after context cancellation")
	}
}

func (s *DataMigrationTestSuite) TestLiveIngestionSkipsSetupClassifiedSEP41WithoutProtocolCursors() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	models := s.setupModels(pool)

	latestLedger, err := models.IngestStore.Get(ctx, "latest_ingest_ledger")
	s.Require().NoError(err)
	s.Require().Greater(latestLedger, uint32(0), "live ingest should have populated latest_ingest_ledger")
	testLedger := latestLedger + 1000

	s.runSEP41ProtocolSetup(ctx, pool, models)

	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, sep41ProtocolID)
	s.Require().NoError(err)
	s.Require().NotEmpty(classifiedContracts, "setup-classified SEP41 contracts should be queryable for live ingestion")

	const cursorName = data.LatestLedgerCursorName
	s.upsertIngestStoreValue(ctx, pool, cursorName, testLedger-1)

	processor := &integrationTestProcessor{id: sep41ProtocolID, ingestStore: models.IngestStore}
	rpcService := services.NewRPCServiceMock(s.T())
	rpcService.On("GetHealth").Return(entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: testLedger,
		OldestLedger: 1,
	}, nil).Once()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	ledgerBackend := &singleLedgerBackend{
		ledgerSeq:  testLedger,
		ledgerMeta: s.mustLedgerCloseMeta(),
	}
	svc := s.newLiveRunService(models, rpcService, ledgerBackend, m, processor)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- svc.Run(runCtx, 0, 0)
	}()

	var earlyRunErr error
	require.Eventually(s.T(), func() bool {
		select {
		case earlyRunErr = <-runErrCh:
			return true
		default:
		}

		cursor, err := models.IngestStore.Get(ctx, cursorName)
		return err == nil && cursor == testLedger
	}, 10*time.Second, 100*time.Millisecond)

	s.Require().NoError(earlyRunErr, "live Run exited before the main ingest cursor advanced")
	s.Assert().Zero(processor.processedLedger, "ProcessLedger should be skipped while protocol cursors do not exist")
	s.Assert().Empty(processor.seenContracts, "live ingestion should skip protocol contract analysis when persistence is definitely gated")

	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "protocol_SEP41_history_cursor"), "history cursor should not be auto-created")
	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "protocol_SEP41_current_state_cursor"), "current-state cursor should not be auto-created")
	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "test_SEP41_history_written"), "history sentinel should not be written without cursors")
	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "test_SEP41_current_state_written"), "current-state sentinel should not be written without cursors")

	cancel()

	select {
	case err := <-runErrCh:
		s.Require().Error(err)
		s.Require().ErrorIs(err, context.Canceled)
	case <-time.After(5 * time.Second):
		s.FailNow("timed out waiting for live Run to stop after context cancellation")
	}
}

func (s *DataMigrationTestSuite) TestLiveIngestionSkipsSetupClassifiedSEP41WhenProtocolCursorsLag() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	models := s.setupModels(pool)

	latestLedger, err := models.IngestStore.Get(ctx, "latest_ingest_ledger")
	s.Require().NoError(err)
	s.Require().Greater(latestLedger, uint32(0), "live ingest should have populated latest_ingest_ledger")
	testLedger := latestLedger + 1000

	s.runSEP41ProtocolSetup(ctx, pool, models)

	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, sep41ProtocolID)
	s.Require().NoError(err)
	s.Require().NotEmpty(classifiedContracts, "setup-classified SEP41 contracts should be queryable for live ingestion")

	const cursorName = data.LatestLedgerCursorName
	s.upsertIngestStoreValue(ctx, pool, cursorName, testLedger-1)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_history_cursor", testLedger-2)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_current_state_cursor", testLedger-2)

	processor := &integrationTestProcessor{id: sep41ProtocolID, ingestStore: models.IngestStore}
	rpcService := services.NewRPCServiceMock(s.T())
	rpcService.On("GetHealth").Return(entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: testLedger,
		OldestLedger: 1,
	}, nil).Once()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	ledgerBackend := &singleLedgerBackend{
		ledgerSeq:  testLedger,
		ledgerMeta: s.mustLedgerCloseMeta(),
	}
	svc := s.newLiveRunService(models, rpcService, ledgerBackend, m, processor)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- svc.Run(runCtx, 0, 0)
	}()

	var earlyRunErr error
	require.Eventually(s.T(), func() bool {
		select {
		case earlyRunErr = <-runErrCh:
			return true
		default:
		}

		cursor, err := models.IngestStore.Get(ctx, cursorName)
		return err == nil && cursor == testLedger
	}, 10*time.Second, 100*time.Millisecond)

	s.Require().NoError(earlyRunErr, "live Run exited before the main ingest cursor advanced")
	s.Assert().Zero(processor.processedLedger, "ProcessLedger should be skipped while protocol cursors lag behind live ingestion")
	s.Assert().Empty(processor.seenContracts, "live ingestion should skip protocol contract analysis while protocol persistence remains ineligible")

	historyCursor, err := models.IngestStore.Get(ctx, "protocol_SEP41_history_cursor")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger-2, historyCursor, "history cursor should remain unchanged when precheck skips protocol processing")

	currentStateCursor, err := models.IngestStore.Get(ctx, "protocol_SEP41_current_state_cursor")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger-2, currentStateCursor, "current-state cursor should remain unchanged when precheck skips protocol processing")

	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "test_SEP41_history_written"), "history sentinel should not be written while protocol cursors lag")
	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "test_SEP41_current_state_written"), "current-state sentinel should not be written while protocol cursors lag")

	cancel()

	select {
	case err := <-runErrCh:
		s.Require().Error(err)
		s.Require().ErrorIs(err, context.Canceled)
	case <-time.After(5 * time.Second):
		s.FailNow("timed out waiting for live Run to stop after context cancellation")
	}
}

func (s *DataMigrationTestSuite) TestHistoryMigrationThenLiveIngestionHandoff() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	models := s.setupModels(pool)

	latestLedger, err := models.IngestStore.Get(ctx, "latest_ingest_ledger")
	s.Require().NoError(err)
	s.Require().Greater(latestLedger, uint32(0))
	baseSeq := latestLedger + 2000

	// Phase 1: Protocol setup — classify contracts, verify no protocol cursors yet.
	s.runSEP41ProtocolSetup(ctx, pool, models)

	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, sep41ProtocolID)
	s.Require().NoError(err)
	s.Require().NotEmpty(classifiedContracts, "setup should classify at least one SEP41 contract")
	expectedContractKeys := protocolContractKeys(classifiedContracts)

	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "protocol_SEP41_history_cursor"), "protocol cursors should not exist after setup")
	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "protocol_SEP41_current_state_cursor"), "protocol cursors should not exist after setup")

	// Phase 2: History migration — backfill 3 ledgers [baseSeq, baseSeq+2].
	s.upsertIngestStoreValue(ctx, pool, data.OldestLedgerCursorName, baseSeq)
	s.upsertIngestStoreValue(ctx, pool, data.LatestLedgerCursorName, baseSeq+2)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_current_state_cursor", baseSeq+2)

	processor := &integrationTestProcessor{id: sep41ProtocolID, ingestStore: models.IngestStore}

	rangeBackend := &rangeLedgerBackend{
		startSeq:   baseSeq,
		endSeq:     baseSeq + 2,
		ledgerMeta: s.mustLedgerCloseMeta(),
	}
	// When the migration requests the first ledger past the range, simulate
	// live ingestion racing ahead by advancing the cursor. This triggers a CAS
	// failure, handing off the tracker and letting the migration exit cleanly.
	rangeBackend.onMiss = func(seq uint32) {
		//nolint:errcheck
		pool.Exec(ctx,
			`UPDATE ingest_store SET value = $1 WHERE key = $2`,
			strconv.FormatUint(uint64(seq+100), 10), "protocol_SEP41_history_cursor")
	}

	migrationSvc := s.newHistoryMigrationService(
		pool, models, rangeBackend, processor,
	)

	err = migrationSvc.Run(ctx, []string{sep41ProtocolID})
	s.Require().NoError(err, "history migration should complete successfully")

	historyWritten, err := models.IngestStore.Get(ctx, "test_SEP41_history_written")
	s.Require().NoError(err)
	s.Assert().Equal(baseSeq+2, historyWritten, "PersistHistory should have committed data through the last migrated ledger")

	protocols, err := models.Protocols.GetByIDs(ctx, []string{sep41ProtocolID})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].HistoryMigrationStatus, "history migration status should be success")

	s.Assert().NotEmpty(processor.seenContracts, "processor should have seen classified contracts during history migration")
	s.Assert().Equal(expectedContractKeys, protocolContractKeys(processor.seenContracts))
	s.Assert().Equal([]uint32{baseSeq, baseSeq + 1, baseSeq + 2}, processor.persistedHistorySeqs,
		"PersistHistory should be called for every ledger in the migration range")

	// Phase 3: Live ingestion handoff — process baseSeq+3, proving CAS picks up where migration left off.
	// Reset the history cursor to baseSeq+2 (the onMiss callback advanced it
	// past this point to simulate live ingestion handoff during migration).
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_history_cursor", baseSeq+2)
	s.upsertIngestStoreValue(ctx, pool, data.LatestLedgerCursorName, baseSeq+2)

	processor.processedLedger = 0
	processor.seenContracts = nil
	processor.persistedHistorySeqs = nil
	processor.persistedCurrentStateSeqs = nil

	liveBackend := &singleLedgerBackend{
		ledgerSeq:  baseSeq + 3,
		ledgerMeta: s.mustLedgerCloseMeta(),
	}

	rpcService := services.NewRPCServiceMock(s.T())
	rpcService.On("GetHealth").Return(entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: baseSeq + 3,
		OldestLedger: 1,
	}, nil).Once()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	liveSvc := s.newLiveRunService(models, rpcService, liveBackend, m, processor)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- liveSvc.Run(runCtx, 0, 0)
	}()

	var earlyRunErr error
	require.Eventually(s.T(), func() bool {
		select {
		case earlyRunErr = <-runErrCh:
			return true
		default:
		}

		hc, err := models.IngestStore.Get(ctx, "protocol_SEP41_history_cursor")
		if err != nil || hc != baseSeq+3 {
			return false
		}

		hw, err := models.IngestStore.Get(ctx, "test_SEP41_history_written")
		if err != nil || hw != baseSeq+3 {
			return false
		}

		csc, err := models.IngestStore.Get(ctx, "protocol_SEP41_current_state_cursor")
		if err != nil || csc != baseSeq+3 {
			return false
		}

		csw, err := models.IngestStore.Get(ctx, "test_SEP41_current_state_written")
		return err == nil && csw == baseSeq+3
	}, 10*time.Second, 100*time.Millisecond)

	s.Require().NoError(earlyRunErr, "live Run exited before the expected DB state was committed")
	s.Assert().Equal(baseSeq+3, processor.processedLedger, "live ingestion should have processed the handoff ledger")
	s.Assert().Equal(expectedContractKeys, protocolContractKeys(processor.seenContracts), "live ingestion should see the same classified contracts")
	s.Assert().Equal([]uint32{baseSeq + 3}, processor.persistedHistorySeqs,
		"live ingestion should call PersistHistory for the handoff ledger")
	s.Assert().Equal([]uint32{baseSeq + 3}, processor.persistedCurrentStateSeqs,
		"live ingestion should call PersistCurrentState for the handoff ledger")

	cancel()

	select {
	case err := <-runErrCh:
		s.Require().Error(err)
		s.Require().ErrorIs(err, context.Canceled)
	case <-time.After(5 * time.Second):
		s.FailNow("timed out waiting for live Run to stop after context cancellation")
	}
}

func (s *DataMigrationTestSuite) TestLiveIngestionHistoryCursorReadyCurrentStateLags() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	models := s.setupModels(pool)

	latestLedger, err := models.IngestStore.Get(ctx, "latest_ingest_ledger")
	s.Require().NoError(err)
	s.Require().Greater(latestLedger, uint32(0))
	testLedger := latestLedger + 1000

	// Protocol setup — classify contracts.
	s.runSEP41ProtocolSetup(ctx, pool, models)

	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, sep41ProtocolID)
	s.Require().NoError(err)
	s.Require().NotEmpty(classifiedContracts)

	// Set up asymmetric cursors:
	// history_cursor = testLedger-1 → ready (CAS expected matches)
	// current_state_cursor = testLedger-2 → lags (CAS expected=testLedger-1 ≠ testLedger-2)
	const cursorName = data.LatestLedgerCursorName
	s.upsertIngestStoreValue(ctx, pool, cursorName, testLedger-1)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_history_cursor", testLedger-1)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_current_state_cursor", testLedger-2)

	processor := &integrationTestProcessor{id: sep41ProtocolID, ingestStore: models.IngestStore}
	rpcService := services.NewRPCServiceMock(s.T())
	rpcService.On("GetHealth").Return(entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: testLedger,
		OldestLedger: 1,
	}, nil).Once()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	ledgerBackend := &singleLedgerBackend{
		ledgerSeq:  testLedger,
		ledgerMeta: s.mustLedgerCloseMeta(),
	}
	svc := s.newLiveRunService(models, rpcService, ledgerBackend, m, processor)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- svc.Run(runCtx, 0, 0)
	}()

	var earlyRunErr error
	require.Eventually(s.T(), func() bool {
		select {
		case earlyRunErr = <-runErrCh:
			return true
		default:
		}

		cursor, err := models.IngestStore.Get(ctx, cursorName)
		return err == nil && cursor == testLedger
	}, 10*time.Second, 100*time.Millisecond)

	s.Require().NoError(earlyRunErr, "live Run exited before the main ingest cursor advanced")

	// ProcessLedger WAS called — OR precheck passed because history cursor was ready.
	s.Assert().Equal(testLedger, processor.processedLedger,
		"ProcessLedger should run when at least one protocol cursor is ready (OR precheck)")

	// History CAS succeeded — cursor advanced and PersistHistory was called.
	historyCursor, err := models.IngestStore.Get(ctx, "protocol_SEP41_history_cursor")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger, historyCursor,
		"history cursor should advance when its CAS succeeds independently")

	historyWritten, err := models.IngestStore.Get(ctx, "test_SEP41_history_written")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger, historyWritten,
		"PersistHistory should commit when history CAS succeeds")
	s.Assert().Equal([]uint32{testLedger}, processor.persistedHistorySeqs,
		"PersistHistory should be called exactly once for the processed ledger")

	// Current-state CAS failed — cursor unchanged, PersistCurrentState never called.
	currentStateCursor, err := models.IngestStore.Get(ctx, "protocol_SEP41_current_state_cursor")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger-2, currentStateCursor,
		"current-state cursor should remain unchanged when CAS rejects mismatched expected value")

	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "test_SEP41_current_state_written"),
		"PersistCurrentState should not be called when current-state CAS fails")
	s.Assert().Empty(processor.persistedCurrentStateSeqs,
		"persistedCurrentStateSeqs should be empty when current-state CAS fails")

	cancel()

	select {
	case err := <-runErrCh:
		s.Require().Error(err)
		s.Require().ErrorIs(err, context.Canceled)
	case <-time.After(5 * time.Second):
		s.FailNow("timed out waiting for live Run to stop after context cancellation")
	}
}

func (s *DataMigrationTestSuite) newCurrentStateMigrationService(
	pool *pgxpool.Pool,
	models *data.Models,
	ledgerBackend ledgerbackend.LedgerBackend,
	processor services.ProtocolProcessor,
	startLedger uint32,
) services.ProtocolMigrateCurrentStateService {
	svc, err := services.NewProtocolMigrateCurrentStateService(services.ProtocolMigrateCurrentStateConfig{
		DB:                     pool,
		LedgerBackend:          ledgerBackend,
		ProtocolsModel:         models.Protocols,
		ProtocolContractsModel: models.ProtocolContracts,
		IngestStore:            models.IngestStore,
		NetworkPassphrase:      "Test SDF Network ; September 2015",
		Processors:             []services.ProtocolProcessor{processor},
		StartLedger:            startLedger,
	})
	s.Require().NoError(err)
	return svc
}

func (s *DataMigrationTestSuite) TestCurrentStateMigrationThenLiveIngestionHandoff() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()

	models := s.setupModels(pool)

	latestLedger, err := models.IngestStore.Get(ctx, "latest_ingest_ledger")
	s.Require().NoError(err)
	s.Require().Greater(latestLedger, uint32(0))
	baseSeq := latestLedger + 2000

	// Phase 1: Protocol setup — classify contracts, verify no protocol cursors yet.
	s.runSEP41ProtocolSetup(ctx, pool, models)

	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, sep41ProtocolID)
	s.Require().NoError(err)
	s.Require().NotEmpty(classifiedContracts, "setup should classify at least one SEP41 contract")
	expectedContractKeys := protocolContractKeys(classifiedContracts)

	s.Assert().False(s.ingestStoreKeyExists(ctx, pool, "protocol_SEP41_current_state_cursor"), "current-state cursor should not exist after setup")

	// Phase 2: Current-state migration — build current state for 3 ledgers [baseSeq, baseSeq+2].
	s.upsertIngestStoreValue(ctx, pool, data.LatestLedgerCursorName, baseSeq+2)
	// Pre-set history cursor so live ingestion phase can produce history too
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_history_cursor", baseSeq+2)

	processor := &integrationTestProcessor{id: sep41ProtocolID, ingestStore: models.IngestStore}

	rangeBackend := &rangeLedgerBackend{
		startSeq:   baseSeq,
		endSeq:     baseSeq + 2,
		ledgerMeta: s.mustLedgerCloseMeta(),
	}
	// When the migration requests the first ledger past the range, simulate
	// live ingestion racing ahead by advancing the cursor. This triggers a CAS
	// failure, handing off the tracker and letting the migration exit cleanly.
	rangeBackend.onMiss = func(seq uint32) {
		//nolint:errcheck
		pool.Exec(ctx,
			`UPDATE ingest_store SET value = $1 WHERE key = $2`,
			strconv.FormatUint(uint64(seq+100), 10), "protocol_SEP41_current_state_cursor")
	}

	migrationSvc := s.newCurrentStateMigrationService(
		pool, models, rangeBackend, processor, baseSeq,
	)

	err = migrationSvc.Run(ctx, []string{sep41ProtocolID})
	s.Require().NoError(err, "current-state migration should complete successfully")

	currentStateWritten, err := models.IngestStore.Get(ctx, "test_SEP41_current_state_written")
	s.Require().NoError(err)
	s.Assert().Equal(baseSeq+2, currentStateWritten, "PersistCurrentState should have committed data through the last migrated ledger")

	protocols, err := models.Protocols.GetByIDs(ctx, []string{sep41ProtocolID})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].CurrentStateMigrationStatus, "current-state migration status should be success")

	s.Assert().NotEmpty(processor.seenContracts, "processor should have seen classified contracts during current-state migration")
	s.Assert().Equal(expectedContractKeys, protocolContractKeys(processor.seenContracts))
	s.Assert().Equal([]uint32{baseSeq, baseSeq + 1, baseSeq + 2}, processor.persistedCurrentStateSeqs,
		"PersistCurrentState should be called for every ledger in the migration range")

	// Phase 3: Live ingestion handoff — process baseSeq+3, proving CAS picks up where migration left off.
	// Reset the current-state cursor to baseSeq+2 (the onMiss callback advanced it
	// past this point to simulate live ingestion handoff during migration).
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_current_state_cursor", baseSeq+2)
	s.upsertIngestStoreValue(ctx, pool, data.LatestLedgerCursorName, baseSeq+2)

	processor.processedLedger = 0
	processor.seenContracts = nil
	processor.persistedHistorySeqs = nil
	processor.persistedCurrentStateSeqs = nil

	liveBackend := &singleLedgerBackend{
		ledgerSeq:  baseSeq + 3,
		ledgerMeta: s.mustLedgerCloseMeta(),
	}

	rpcService := services.NewRPCServiceMock(s.T())
	rpcService.On("GetHealth").Return(entities.RPCGetHealthResult{
		Status:       "healthy",
		LatestLedger: baseSeq + 3,
		OldestLedger: 1,
	}, nil).Once()

	m := metrics.NewMetrics(prometheus.NewRegistry())
	liveSvc := s.newLiveRunService(models, rpcService, liveBackend, m, processor)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- liveSvc.Run(runCtx, 0, 0)
	}()

	var earlyRunErr error
	require.Eventually(s.T(), func() bool {
		select {
		case earlyRunErr = <-runErrCh:
			return true
		default:
		}

		hc, err := models.IngestStore.Get(ctx, "protocol_SEP41_history_cursor")
		if err != nil || hc != baseSeq+3 {
			return false
		}

		hw, err := models.IngestStore.Get(ctx, "test_SEP41_history_written")
		if err != nil || hw != baseSeq+3 {
			return false
		}

		csc, err := models.IngestStore.Get(ctx, "protocol_SEP41_current_state_cursor")
		if err != nil || csc != baseSeq+3 {
			return false
		}

		csw, err := models.IngestStore.Get(ctx, "test_SEP41_current_state_written")
		return err == nil && csw == baseSeq+3
	}, 10*time.Second, 100*time.Millisecond)

	s.Require().NoError(earlyRunErr, "live Run exited before the expected DB state was committed")
	s.Assert().Equal(baseSeq+3, processor.processedLedger, "live ingestion should have processed the handoff ledger")
	s.Assert().Equal(expectedContractKeys, protocolContractKeys(processor.seenContracts), "live ingestion should see the same classified contracts")
	s.Assert().Equal([]uint32{baseSeq + 3}, processor.persistedHistorySeqs,
		"live ingestion should call PersistHistory for the handoff ledger")
	s.Assert().Equal([]uint32{baseSeq + 3}, processor.persistedCurrentStateSeqs,
		"live ingestion should call PersistCurrentState for the handoff ledger")

	cancel()

	select {
	case err := <-runErrCh:
		s.Require().Error(err)
		s.Require().ErrorIs(err, context.Canceled)
	case <-time.After(5 * time.Second):
		s.FailNow("timed out waiting for live Run to stop after context cancellation")
	}
}

func TestDataMigrationTestSuiteStandalone(t *testing.T) {
	t.Skip("Run via TestIntegrationTests")
}
