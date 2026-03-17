package integrationtests

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
)

// --- ProtocolSetupTestSuite (requires Docker DB + ingest-populated protocol_wasms) ---

type ProtocolSetupTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (s *ProtocolSetupTestSuite) setupDB() (db.ConnectionPool, func()) {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)
	pool, err := db.OpenDBConnectionPool(dbURL)
	s.Require().NoError(err)
	return pool, func() { pool.Close() }
}

func (s *ProtocolSetupTestSuite) setupModels(pool db.ConnectionPool) (*data.ProtocolsModel, *data.ProtocolWasmsModel) {
	mockMetrics := metrics.NewMockMetricsService()
	mockMetrics.On("ObserveDBQueryDuration", mock.Anything, mock.Anything, mock.Anything).Return()
	mockMetrics.On("ObserveDBBatchSize", mock.Anything, mock.Anything, mock.Anything).Return()
	mockMetrics.On("IncDBQuery", mock.Anything, mock.Anything).Return()
	mockMetrics.On("IncDBQueryError", mock.Anything, mock.Anything, mock.Anything).Return()
	return &data.ProtocolsModel{DB: pool, MetricsService: mockMetrics},
		&data.ProtocolWasmsModel{DB: pool, MetricsService: mockMetrics}
}

// SetupTest resets classification state before each test, preserving ingest-populated rows.
func (s *ProtocolSetupTestSuite) SetupTest() {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)

	sqlDB, err := sql.Open("postgres", dbURL)
	s.Require().NoError(err)
	defer sqlDB.Close()

	// Reset classification: set all protocol_id back to NULL (preserves ingest rows)
	_, err = sqlDB.ExecContext(ctx, "UPDATE protocol_wasms SET protocol_id = NULL")
	s.Require().NoError(err)
	// Remove protocol rows so Run() can insert fresh
	_, err = sqlDB.ExecContext(ctx, "DELETE FROM protocols")
	s.Require().NoError(err)
}

func (s *ProtocolSetupTestSuite) TestProtocolSetupClassifiesIngestedWasms() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()
	protocolModel, protocolWasmModel := s.setupModels(pool)

	// 1. Verify ingest populated unclassified WASMs (sanity check)
	unclassifiedBefore, err := protocolWasmModel.GetUnclassified(ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(unclassifiedBefore, "ingest should have populated protocol_wasms with unclassified entries")
	totalWasms := len(unclassifiedBefore)
	s.T().Logf("Found %d unclassified WASMs from ingest", totalWasms)

	// 2. Insert the SEP41 protocol row
	err = db.RunInPgxTransaction(ctx, pool, func(dbTx pgx.Tx) error {
		return protocolModel.InsertIfNotExists(ctx, dbTx, "SEP41")
	})
	s.Require().NoError(err)

	// 3. Create ProtocolSetupService with real dependencies (no mocks)
	specExtractor := services.NewWasmSpecExtractor()
	validator := services.NewSEP41ProtocolValidator()

	svc := services.NewProtocolSetupService(
		pool, s.testEnv.RPCService, protocolModel, protocolWasmModel,
		specExtractor, []services.ProtocolValidator{validator},
	)

	// 4. Run the full classification pipeline
	s.Require().NoError(svc.Run(ctx, []string{"SEP41"}))

	// 5. Assert: at least one WASM was classified as SEP41
	//    (the token contract WASM hash should now have protocol_id = "SEP41")
	unclassifiedAfter, err := protocolWasmModel.GetUnclassified(ctx)
	s.Require().NoError(err)
	classifiedCount := totalWasms - len(unclassifiedAfter)
	s.Assert().Greater(classifiedCount, 0, "at least one WASM should have been classified as SEP41")
	s.T().Logf("Classified %d/%d WASMs as SEP41", classifiedCount, totalWasms)

	// 6. Assert: protocol classification_status = "success"
	protocols, err := protocolModel.GetByIDs(ctx, []string{"SEP41"})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].ClassificationStatus)

	var cursorCount int
	err = pool.GetContext(ctx, &cursorCount,
		`SELECT COUNT(*) FROM ingest_store WHERE key IN ($1, $2)`,
		"protocol_SEP41_history_cursor", "protocol_SEP41_current_state_cursor")
	s.Require().NoError(err)
	s.Assert().Zero(cursorCount, "protocol-setup should not initialize protocol cursors")

	// 7. Assert: some WASMs remain unclassified (increment contract, SAC WASMs, etc.)
	s.Assert().NotEmpty(unclassifiedAfter, "some WASMs (increment contract, SAC WASMs) should remain unclassified")
	s.T().Logf("%d WASMs remain unclassified", len(unclassifiedAfter))
}

// Ensure ProtocolSetupTestSuite implements the suite.SetupTestSuite interface
var _ suite.SetupTestSuite = (*ProtocolSetupTestSuite)(nil)

func TestProtocolSetupTestSuiteStandalone(t *testing.T) {
	// This is only run via TestIntegrationTests in main_test.go
	t.Skip("Run via TestIntegrationTests")
}
