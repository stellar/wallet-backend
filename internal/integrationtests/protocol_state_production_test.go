package integrationtests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/apptracker"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

// --- ProtocolStateProductionTestSuite (requires Docker DB + live ingest) ---

type ProtocolStateProductionTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (s *ProtocolStateProductionTestSuite) setupDB() (db.ConnectionPool, func()) {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)
	pool, err := db.OpenDBConnectionPool(dbURL)
	s.Require().NoError(err)
	return pool, func() { pool.Close() }
}

func (s *ProtocolStateProductionTestSuite) setupModels(pool db.ConnectionPool) *data.Models {
	mockMetrics := metrics.NewMockMetricsService()
	mockMetrics.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
	mockMetrics.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
	mockMetrics.On("IncDBQuery", mock.Anything, mock.Anything).Return()
	mockMetrics.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return()
	mockMetrics.On("RegisterPoolMetrics", mock.Anything, mock.Anything).Return()
	models, err := data.NewModels(pool, mockMetrics)
	s.Require().NoError(err)
	return models
}

func (s *ProtocolStateProductionTestSuite) cleanupTestKeys(ctx context.Context, dbURL string) {
	sqlDB, err := sql.Open("postgres", dbURL)
	s.Require().NoError(err)
	defer sqlDB.Close()

	_, err = sqlDB.ExecContext(ctx, `DELETE FROM ingest_store WHERE key LIKE 'test_%' OR key LIKE 'protocol_testproto%'`)
	s.Require().NoError(err)
}

// integrationTestProcessor implements services.ProtocolProcessor using the real
// IngestStoreModel to write sentinel keys within the DB transaction.
type integrationTestProcessor struct {
	id              string
	processedLedger uint32
	ingestStore     *data.IngestStoreModel
}

func (p *integrationTestProcessor) ProtocolID() string { return p.id }

func (p *integrationTestProcessor) ProcessLedger(_ context.Context, input services.ProtocolProcessorInput) error {
	p.processedLedger = input.LedgerSequence
	return nil
}

func (p *integrationTestProcessor) PersistHistory(ctx context.Context, dbTx pgx.Tx) error {
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_history_written", p.id), p.processedLedger)
}

func (p *integrationTestProcessor) PersistCurrentState(ctx context.Context, dbTx pgx.Tx) error {
	return p.ingestStore.Update(ctx, dbTx, fmt.Sprintf("test_%s_current_state_written", p.id), p.processedLedger)
}

// TestDualCASGatingDuringLiveIngestion proves CAS gating works against the
// Docker DB that has been populated by the real ingest container.
func (s *ProtocolStateProductionTestSuite) TestDualCASGatingDuringLiveIngestion() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()
	models := s.setupModels(pool)

	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)
	defer s.cleanupTestKeys(ctx, dbURL)

	// Read current latest_ingest_ledger to know where the live container is
	latestLedger, err := models.IngestStore.Get(ctx, "latest_ingest_ledger")
	s.Require().NoError(err)
	s.Require().Greater(latestLedger, uint32(0), "live ingest should have populated latest_ingest_ledger")
	s.T().Logf("Live ingest container is at ledger %d", latestLedger)

	// Pick a test ledger well beyond the live ingest tip to avoid collision
	testLedger := latestLedger + 1000

	// Insert protocol cursors at testLedger-1 (ready for CAS win)
	sqlDB, err := sql.Open("postgres", dbURL)
	s.Require().NoError(err)
	defer sqlDB.Close()

	_, err = sqlDB.ExecContext(ctx,
		`INSERT INTO ingest_store (key, value) VALUES ($1, $2)`,
		"protocol_testproto_history_cursor", testLedger-1)
	s.Require().NoError(err)
	_, err = sqlDB.ExecContext(ctx,
		`INSERT INTO ingest_store (key, value) VALUES ($1, $2)`,
		"protocol_testproto_current_state_cursor", testLedger-1)
	s.Require().NoError(err)

	// Insert a test-specific main cursor (avoid interfering with real ingest)
	_, err = sqlDB.ExecContext(ctx,
		`INSERT INTO ingest_store (key, value) VALUES ($1, $2)`,
		"test_cursor", testLedger-1)
	s.Require().NoError(err)

	processor := &integrationTestProcessor{id: "testproto", ingestStore: models.IngestStore, processedLedger: testLedger}

	mockTokenIngestionService := services.NewTokenIngestionServiceMock(s.T())
	mockTokenIngestionService.On("ProcessTokenChanges",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(nil).Maybe()

	mockMetrics := metrics.NewMockMetricsService()
	mockMetrics.On("RegisterPoolMetrics", mock.Anything, mock.Anything).Return()
	mockMetrics.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("IncDBQuery", mock.Anything, mock.Anything).Return().Maybe()
	mockMetrics.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()

	svc, err := services.NewIngestService(services.IngestServiceConfig{
		IngestionMode:          services.IngestionModeLive,
		Models:                 models,
		LatestLedgerCursorName: "test_cursor",
		OldestLedgerCursorName: "test_cursor",
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             s.testEnv.RPCService,
		LedgerBackend:          &services.LedgerBackendMock{},
		ChannelAccountStore:    &store.ChannelAccountStoreMock{},
		TokenIngestionService:  mockTokenIngestionService,
		MetricsService:         mockMetrics,
		Network:                "Test SDF Network ; September 2015",
		NetworkPassphrase:      "Test SDF Network ; September 2015",
		Archive:                &services.HistoryArchiveMock{},
		ProtocolProcessors:     []services.ProtocolProcessor{processor},
	})
	s.Require().NoError(err)

	buffer := indexer.NewIndexerBuffer()

	// Phase 1: CAS Win — cursors at testLedger-1, persisting testLedger
	s.T().Log("Phase 1: CAS win")
	_, _, err = svc.PersistLedgerData(ctx, testLedger, buffer, "test_cursor")
	s.Require().NoError(err)

	histCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger, histCursor, "history cursor should advance to testLedger")

	csCursor, err := models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger, csCursor, "current state cursor should advance to testLedger")

	histSentinel, err := models.IngestStore.Get(ctx, "test_testproto_history_written")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger, histSentinel, "history sentinel should be testLedger")

	csSentinel, err := models.IngestStore.Get(ctx, "test_testproto_current_state_written")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger, csSentinel, "current state sentinel should be testLedger")

	// Phase 2: CAS Lose — same ledger again, CAS expects testLedger-1 but finds testLedger
	s.T().Log("Phase 2: CAS lose (same ledger again)")
	_, _, err = svc.PersistLedgerData(ctx, testLedger, buffer, "test_cursor")
	s.Require().NoError(err)

	// Cursors should still be at testLedger
	histCursor, err = models.IngestStore.Get(ctx, "protocol_testproto_history_cursor")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger, histCursor, "history cursor should remain at testLedger after CAS lose")

	csCursor, err = models.IngestStore.Get(ctx, "protocol_testproto_current_state_cursor")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger, csCursor, "current state cursor should remain at testLedger after CAS lose")

	// Delete sentinels, re-run, and verify they are NOT re-written
	_, err = sqlDB.ExecContext(ctx, `DELETE FROM ingest_store WHERE key LIKE 'test_testproto_%_written'`)
	s.Require().NoError(err)

	_, _, err = svc.PersistLedgerData(ctx, testLedger, buffer, "test_cursor")
	s.Require().NoError(err)

	histSentinel, err = models.IngestStore.Get(ctx, "test_testproto_history_written")
	s.Require().NoError(err)
	s.Assert().Equal(uint32(0), histSentinel, "sentinels should NOT be re-written after CAS lose")

	csSentinel, err = models.IngestStore.Get(ctx, "test_testproto_current_state_written")
	s.Require().NoError(err)
	s.Assert().Equal(uint32(0), csSentinel, "sentinels should NOT be re-written after CAS lose")

	// Phase 3: Cursor behind — second protocol at testLedger-2
	s.T().Log("Phase 3: Cursor behind (second protocol)")
	_, err = sqlDB.ExecContext(ctx,
		`INSERT INTO ingest_store (key, value) VALUES ($1, $2)`,
		"protocol_testproto2_history_cursor", testLedger-2)
	s.Require().NoError(err)
	_, err = sqlDB.ExecContext(ctx,
		`INSERT INTO ingest_store (key, value) VALUES ($1, $2)`,
		"protocol_testproto2_current_state_cursor", testLedger-2)
	s.Require().NoError(err)

	processor2 := &integrationTestProcessor{id: "testproto2", ingestStore: models.IngestStore, processedLedger: testLedger}

	svc2, err := services.NewIngestService(services.IngestServiceConfig{
		IngestionMode:          services.IngestionModeLive,
		Models:                 models,
		LatestLedgerCursorName: "test_cursor",
		OldestLedgerCursorName: "test_cursor",
		AppTracker:             &apptracker.MockAppTracker{},
		RPCService:             s.testEnv.RPCService,
		LedgerBackend:          &services.LedgerBackendMock{},
		ChannelAccountStore:    &store.ChannelAccountStoreMock{},
		TokenIngestionService:  mockTokenIngestionService,
		MetricsService:         mockMetrics,
		Network:                "Test SDF Network ; September 2015",
		NetworkPassphrase:      "Test SDF Network ; September 2015",
		Archive:                &services.HistoryArchiveMock{},
		ProtocolProcessors:     []services.ProtocolProcessor{processor2},
	})
	s.Require().NoError(err)

	_, _, err = svc2.PersistLedgerData(ctx, testLedger, buffer, "test_cursor")
	s.Require().NoError(err)

	hist2, err := models.IngestStore.Get(ctx, "protocol_testproto2_history_cursor")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger-2, hist2, "testproto2 history cursor should stay behind")

	cs2, err := models.IngestStore.Get(ctx, "protocol_testproto2_current_state_cursor")
	s.Require().NoError(err)
	s.Assert().Equal(testLedger-2, cs2, "testproto2 current state cursor should stay behind")

	histSentinel2, err := models.IngestStore.Get(ctx, "test_testproto2_history_written")
	s.Require().NoError(err)
	s.Assert().Equal(uint32(0), histSentinel2, "no sentinels for behind protocol")

	csSentinel2, err := models.IngestStore.Get(ctx, "test_testproto2_current_state_written")
	s.Require().NoError(err)
	s.Assert().Equal(uint32(0), csSentinel2, "no sentinels for behind protocol")
}

func TestProtocolStateProductionTestSuiteStandalone(t *testing.T) {
	t.Skip("Run via TestIntegrationTests")
}
