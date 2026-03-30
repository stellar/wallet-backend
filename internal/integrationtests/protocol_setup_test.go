package integrationtests

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
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

func (s *ProtocolSetupTestSuite) setupDB() (*pgxpool.Pool, func()) {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)
	pool, err := db.OpenDBConnectionPool(ctx, dbURL)
	s.Require().NoError(err)
	return pool, func() { pool.Close() }
}

func (s *ProtocolSetupTestSuite) setupModels(pool *pgxpool.Pool) (*data.ProtocolsModel, *data.ProtocolWasmsModel) {
	dbMetrics := metrics.NewMetrics(prometheus.NewRegistry()).DB
	return &data.ProtocolsModel{DB: pool, Metrics: dbMetrics},
		&data.ProtocolWasmsModel{DB: pool, Metrics: dbMetrics}
}

// SetupTest resets classification state before each test, preserving ingest-populated rows.
func (s *ProtocolSetupTestSuite) SetupTest() {
	ctx := context.Background()
	dbURL, err := s.testEnv.Containers.GetWalletDBConnectionString(ctx)
	s.Require().NoError(err)

	sqlDB, err := sql.Open("postgres", dbURL)
	s.Require().NoError(err)
	defer sqlDB.Close()

	_, err = sqlDB.ExecContext(ctx, "UPDATE protocol_wasms SET protocol_id = NULL")
	s.Require().NoError(err)
	_, err = sqlDB.ExecContext(ctx, "DELETE FROM protocols")
	s.Require().NoError(err)
}

func (s *ProtocolSetupTestSuite) TestProtocolSetupClassifiesIngestedWasms() {
	ctx := context.Background()
	pool, cleanup := s.setupDB()
	defer cleanup()
	protocolModel, protocolWasmModel := s.setupModels(pool)

	unclassifiedBefore, err := protocolWasmModel.GetUnclassified(ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(unclassifiedBefore, "ingest should have populated protocol_wasms with unclassified entries")
	totalWasms := len(unclassifiedBefore)
	s.T().Logf("Found %d unclassified WASMs from ingest", totalWasms)

	err = db.RunInTransaction(ctx, pool, func(dbTx pgx.Tx) error {
		return protocolModel.InsertIfNotExists(ctx, dbTx, "SEP41")
	})
	s.Require().NoError(err)

	specExtractor := services.NewWasmSpecExtractor()
	validator := services.NewSEP41ProtocolValidator()

	svc := services.NewProtocolSetupService(
		pool, s.testEnv.RPCService, protocolModel, protocolWasmModel,
		specExtractor, []services.ProtocolValidator{validator},
	)

	s.Require().NoError(svc.Run(ctx, []string{"SEP41"}))

	unclassifiedAfter, err := protocolWasmModel.GetUnclassified(ctx)
	s.Require().NoError(err)
	classifiedCount := totalWasms - len(unclassifiedAfter)
	s.Assert().Greater(classifiedCount, 0, "at least one WASM should have been classified as SEP41")
	s.T().Logf("Classified %d/%d WASMs as SEP41", classifiedCount, totalWasms)

	protocols, err := protocolModel.GetByIDs(ctx, []string{"SEP41"})
	s.Require().NoError(err)
	s.Require().Len(protocols, 1)
	s.Assert().Equal(data.StatusSuccess, protocols[0].ClassificationStatus)

	s.Assert().NotEmpty(unclassifiedAfter, "some WASMs (increment contract, SAC WASMs) should remain unclassified")
	s.T().Logf("%d WASMs remain unclassified", len(unclassifiedAfter))
}

// Ensure ProtocolSetupTestSuite implements the suite.SetupTestSuite interface
var _ suite.SetupTestSuite = (*ProtocolSetupTestSuite)(nil)

func TestProtocolSetupTestSuiteStandalone(t *testing.T) {
	t.Skip("Run via TestIntegrationTests")
}
