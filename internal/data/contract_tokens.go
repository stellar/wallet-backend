package data

import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type ContractModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

type Contract struct {
	ID        string    `db:"id" json:"id"`
	Type      string    `db:"type" json:"type"`
	Code      *string   `db:"code" json:"code"`
	Issuer    *string   `db:"issuer" json:"issuer"`
	Name      string    `db:"name" json:"name"`
	Symbol    string    `db:"symbol" json:"symbol"`
	Decimals  int16     `db:"decimals" json:"decimals"`
	CreatedAt time.Time `db:"created_at" json:"createdAt"`
	UpdatedAt time.Time `db:"updated_at" json:"updatedAt"`
}

func (m *ContractModel) GetByID(ctx context.Context, contractID string) (*Contract, error) {
	start := time.Now()
	contract := &Contract{}
	err := m.DB.GetContext(ctx, contract, "SELECT * FROM contract_tokens WHERE id = $1", contractID)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetByID", "contract_tokens", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetByID", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting contract by ID %s: %w", contractID, err)
	}
	m.MetricsService.IncDBQuery("GetByID", "contract_tokens")
	return contract, nil
}

// BatchInsert inserts multiple contracts in a single query using UNNEST.
// It returns the IDs of successfully inserted contracts.
// Contracts that already exist (duplicate IDs) are skipped via ON CONFLICT DO NOTHING.
func (m *ContractModel) BatchInsert(ctx context.Context, sqlExecuter db.SQLExecuter, contracts []*Contract) ([]string, error) {
	if len(contracts) == 0 {
		return nil, nil
	}

	if sqlExecuter == nil {
		sqlExecuter = m.DB
	}

	// Flatten contracts into parallel slices
	ids := make([]string, len(contracts))
	types := make([]string, len(contracts))
	codes := make([]*string, len(contracts))
	issuers := make([]*string, len(contracts))
	names := make([]string, len(contracts))
	symbols := make([]string, len(contracts))
	decimals := make([]int16, len(contracts))

	for i, c := range contracts {
		ids[i] = c.ID
		types[i] = c.Type
		codes[i] = c.Code
		issuers[i] = c.Issuer
		names[i] = c.Name
		symbols[i] = c.Symbol
		decimals[i] = c.Decimals
	}

	const insertQuery = `
		WITH inserted_contracts AS (
			INSERT INTO contract_tokens (id, type, code, issuer, name, symbol, decimals)
			SELECT
				c.id, c.type, c.code, c.issuer, c.name, c.symbol, c.decimals
			FROM (
				SELECT
					UNNEST($1::text[]) AS id,
					UNNEST($2::text[]) AS type,
					UNNEST($3::text[]) AS code,
					UNNEST($4::text[]) AS issuer,
					UNNEST($5::text[]) AS name,
					UNNEST($6::text[]) AS symbol,
					UNNEST($7::smallint[]) AS decimals
			) c
			ON CONFLICT (id) DO NOTHING
			RETURNING id
		)
		SELECT id FROM inserted_contracts;
	`

	start := time.Now()
	var insertedIDs []string
	err := sqlExecuter.SelectContext(ctx, &insertedIDs, insertQuery,
		pq.Array(ids),
		pq.Array(types),
		pq.Array(codes),
		pq.Array(issuers),
		pq.Array(names),
		pq.Array(symbols),
		pq.Array(decimals),
	)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "contract_tokens", duration)
	m.MetricsService.ObserveDBBatchSize("BatchInsert", "contract_tokens", len(contracts))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchInsert", "contract_tokens", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("batch inserting contracts: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchInsert", "contract_tokens")

	return insertedIDs, nil
}
