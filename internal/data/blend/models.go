package blend

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// Models aggregates Blend v2 data models so callers can reach them via a single
// field on data.Models (e.g., models.Blend.Pools).
type Models struct {
	Pools             *PoolModel
	Positions         *PositionModel
	Reserves          *ReserveModel
	BackstopPositions *BackstopPositionModel
	BackstopPools     *BackstopPoolModel
	Emissions         *EmissionModel
	ReserveEmissions  *ReserveEmissionModel
	PoolClaimed       *PoolClaimedModel
	BackstopClaimed   *BackstopClaimedModel
	Auctions          *AuctionModel
	OraclePrices      *OraclePriceModel
}

// NewModels constructs the Blend Models aggregate wired to the given pool/metrics.
func NewModels(pool *pgxpool.Pool, dbMetrics *metrics.DBMetrics) Models {
	return Models{
		Pools:             &PoolModel{DB: pool, Metrics: dbMetrics},
		Positions:         &PositionModel{DB: pool, Metrics: dbMetrics},
		Reserves:          &ReserveModel{DB: pool, Metrics: dbMetrics},
		BackstopPositions: &BackstopPositionModel{DB: pool, Metrics: dbMetrics},
		BackstopPools:     &BackstopPoolModel{DB: pool, Metrics: dbMetrics},
		Emissions:         &EmissionModel{DB: pool, Metrics: dbMetrics},
		ReserveEmissions:  &ReserveEmissionModel{DB: pool, Metrics: dbMetrics},
		PoolClaimed:       &PoolClaimedModel{DB: pool, Metrics: dbMetrics},
		BackstopClaimed:   &BackstopClaimedModel{DB: pool, Metrics: dbMetrics},
		Auctions:          &AuctionModel{DB: pool, Metrics: dbMetrics},
		OraclePrices:      &OraclePriceModel{DB: pool, Metrics: dbMetrics},
	}
}
