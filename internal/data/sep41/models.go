package sep41

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// Models aggregates SEP-41 data models so callers can reach them via a single field
// on data.Models (e.g., models.SEP41.Balances).
type Models struct {
	Balances   BalanceModelInterface
	Allowances AllowanceModelInterface
}

// NewModels constructs the SEP-41 Models aggregate wired to the given pool/metrics.
// hiddenContracts (C-addresses) are skipped by every balance and allowance
// read; pass nil outside the API server.
func NewModels(pool *pgxpool.Pool, dbMetrics *metrics.DBMetrics, hiddenContracts []string) Models {
	return Models{
		Balances:   &BalanceModel{DB: pool, Metrics: dbMetrics, HiddenContracts: hiddenContracts},
		Allowances: &AllowanceModel{DB: pool, Metrics: dbMetrics, HiddenContracts: hiddenContracts},
	}
}
