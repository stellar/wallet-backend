// Package data provides data access layer for trustline asset operations.
package data

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// Note: pq is still used by BatchGetByIDs for sqlx compatibility

// DeterministicAssetID computes a deterministic 64-bit ID for a trustline asset
// using FNV-1a hash of "CODE:ISSUER". This enables streaming checkpoint processing
// without needing DB roundtrips to get auto-generated IDs.
func DeterministicAssetID(code, issuer string) int64 {
	h := fnv.New64a()
	h.Write([]byte(code + ":" + issuer))
	return int64(h.Sum64())
}

// TrustlineAssetModelInterface defines the interface for trustline asset operations.
type TrustlineAssetModelInterface interface {
	// BatchInsert inserts multiple trustline assets with pre-computed IDs.
	// Uses INSERT ... ON CONFLICT DO NOTHING for idempotent operations.
	BatchInsert(ctx context.Context, dbTx pgx.Tx, assets []TrustlineAsset) error
	// BatchGetByIDs retrieves trustline assets by their IDs.
	BatchGetByIDs(ctx context.Context, ids []int64) ([]*TrustlineAsset, error)
}

// TrustlineAssetModel implements TrustlineAssetModelInterface.
type TrustlineAssetModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

var _ TrustlineAssetModelInterface = (*TrustlineAssetModel)(nil)

// TrustlineAsset represents a classic Stellar trustline asset.
type TrustlineAsset struct {
	ID        int64     `db:"id" json:"id"`
	Code      string    `db:"code" json:"code"`
	Issuer    string    `db:"issuer" json:"issuer"`
	CreatedAt time.Time `db:"created_at" json:"createdAt"`
}

// AssetKey returns the "CODE:ISSUER" format string for this asset.
func (a *TrustlineAsset) AssetKey() string {
	return fmt.Sprintf("%s:%s", a.Code, a.Issuer)
}

// BatchInsert inserts multiple trustline assets with pre-computed deterministic IDs.
// Uses INSERT ... ON CONFLICT DO NOTHING for idempotent operations.
// Assets must have their ID field set via DeterministicAssetID before calling.
func (m *TrustlineAssetModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, assets []TrustlineAsset) error {
	if len(assets) == 0 {
		return nil
	}

	ids := make([]int64, len(assets))
	codes := make([]string, len(assets))
	issuers := make([]string, len(assets))
	for i, a := range assets {
		ids[i] = a.ID
		codes[i] = a.Code
		issuers[i] = a.Issuer
	}

	const query = `
		INSERT INTO trustline_assets (id, code, issuer)
		SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::text[])
		ON CONFLICT DO NOTHING
	`

	start := time.Now()
	_, err := dbTx.Exec(ctx, query, ids, codes, issuers)
	if err != nil {
		return fmt.Errorf("batch inserting trustline assets: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchInsert", "trustline_assets", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchInsert", "trustline_assets")
	return nil
}

// BatchGetByIDs retrieves trustline assets by their IDs.
// Returns assets in arbitrary order (not necessarily matching input order).
func (m *TrustlineAssetModel) BatchGetByIDs(ctx context.Context, ids []int64) ([]*TrustlineAsset, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	const query = `SELECT id, code, issuer, created_at FROM trustline_assets WHERE id = ANY($1)`

	start := time.Now()
	var assets []*TrustlineAsset
	err := m.DB.SelectContext(ctx, &assets, query, pq.Array(ids))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByIDs", "trustline_assets", duration)

	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByIDs", "trustline_assets", "query_error")
		return nil, fmt.Errorf("batch getting trustline assets by IDs: %w", err)
	}

	m.MetricsService.IncDBQuery("BatchGetByIDs", "trustline_assets")
	return assets, nil
}
