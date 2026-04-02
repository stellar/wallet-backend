package integrationtests

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

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
)

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
		key, value)
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
		return models.Protocols.InsertIfNotExists(ctx, dbTx, "SEP41")
	})
	s.Require().NoError(err)

	specExtractor := services.NewWasmSpecExtractor()
	validator := services.NewSEP41ProtocolValidator()

	svc := services.NewProtocolSetupService(
		pool,
		s.testEnv.RPCService,
		models.Protocols,
		models.ProtocolWasms,
		specExtractor,
		[]services.ProtocolValidator{validator},
	)

	s.Require().NoError(svc.Run(ctx, []string{"SEP41"}))
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

type integrationTestProcessor struct {
	id              string
	processedLedger uint32
	seenContracts   []data.ProtocolContracts
	ingestStore     *data.IngestStoreModel
}

func (p *integrationTestProcessor) ProtocolID() string { return p.id }

func (p *integrationTestProcessor) ProcessLedger(_ context.Context, input services.ProtocolProcessorInput) error {
	p.processedLedger = input.LedgerSequence
	p.seenContracts = append([]data.ProtocolContracts(nil), input.ProtocolContracts...)
	return nil
}

func (p *integrationTestProcessor) PersistHistory(ctx context.Context, dbTx pgx.Tx) error {
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_history_written", p.id), p.processedLedger)
}

func (p *integrationTestProcessor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_current_state_written", p.id), p.processedLedger)
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
	cursorName string,
) services.IngestService {
	svc, err := services.NewIngestService(services.IngestServiceConfig{
		IngestionMode:          services.IngestionModeLive,
		Models:                 models,
		LatestLedgerCursorName: cursorName,
		OldestLedgerCursorName: cursorName,
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

	protocols, err := models.Protocols.GetByIDs(ctx, []string{"SEP41"})
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

	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, "SEP41")
	s.Require().NoError(err)
	s.Require().NotEmpty(classifiedContracts, "setup-classified SEP41 contracts should be queryable for live ingestion")
	expectedContractKeys := protocolContractKeys(classifiedContracts)

	const cursorName = "test_live_run_cursor"
	s.upsertIngestStoreValue(ctx, pool, cursorName, testLedger-1)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_history_cursor", testLedger-1)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_current_state_cursor", testLedger-1)

	processor := &integrationTestProcessor{id: "SEP41", ingestStore: models.IngestStore}
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
	svc := s.newLiveRunService(models, rpcService, ledgerBackend, m, processor, cursorName)

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

	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, "SEP41")
	s.Require().NoError(err)
	s.Require().NotEmpty(classifiedContracts, "setup-classified SEP41 contracts should be queryable for live ingestion")

	const cursorName = "test_live_run_cursor"
	s.upsertIngestStoreValue(ctx, pool, cursorName, testLedger-1)

	processor := &integrationTestProcessor{id: "SEP41", ingestStore: models.IngestStore}
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
	svc := s.newLiveRunService(models, rpcService, ledgerBackend, m, processor, cursorName)

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

	classifiedContracts, err := models.ProtocolContracts.GetByProtocolID(ctx, "SEP41")
	s.Require().NoError(err)
	s.Require().NotEmpty(classifiedContracts, "setup-classified SEP41 contracts should be queryable for live ingestion")

	const cursorName = "test_live_run_cursor"
	s.upsertIngestStoreValue(ctx, pool, cursorName, testLedger-1)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_history_cursor", testLedger-2)
	s.upsertIngestStoreValue(ctx, pool, "protocol_SEP41_current_state_cursor", testLedger-2)

	processor := &integrationTestProcessor{id: "SEP41", ingestStore: models.IngestStore}
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
	svc := s.newLiveRunService(models, rpcService, ledgerBackend, m, processor, cursorName)

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

func TestDataMigrationTestSuiteStandalone(t *testing.T) {
	t.Skip("Run via TestIntegrationTests")
}
