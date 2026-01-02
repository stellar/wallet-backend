// Package data provides data access layer for trustline asset operations.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// TrustlineAssetModelInterface defines the interface for trustline asset operations.
type TrustlineAssetModelInterface interface {
	// BatchGetOrInsert returns IDs for multiple trustline assets, creating any that don't exist.
	BatchGetOrInsert(ctx context.Context, assets []TrustlineAsset) (map[string]int64, error)
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

// BatchGetOrInsert returns IDs for multiple trustline assets, creating any that don't exist.
// Uses batch INSERT ... ON CONFLICT for atomic upsert behavior.
// Returns a map of "code:issuer" -> id.
func (m *TrustlineAssetModel) BatchGetOrInsert(ctx context.Context, assets []TrustlineAsset) (map[string]int64, error) {
	if len(assets) == 0 {
		return make(map[string]int64), nil
	}

	codes := make([]string, len(assets))
	issuers := make([]string, len(assets))
	for i, a := range assets {
		codes[i] = a.Code
		issuers[i] = a.Issuer
	}

	// Step 1: Try to get existing rows first (cheap SELECT)
	const selectQuery = `
		SELECT id, code, issuer FROM trustline_assets
		WHERE (code, issuer) IN (SELECT * FROM UNNEST($1::text[], $2::text[]))
	`

	start := time.Now()
	rows, err := m.DB.QueryxContext(ctx, selectQuery, pq.Array(codes), pq.Array(issuers))
	if err != nil {
		return nil, fmt.Errorf("selecting trustline assets: %w", err)
	}

	result := make(map[string]int64, len(assets))
	for rows.Next() {
		var id int64
		var code, issuer string
		if err := rows.Scan(&id, &code, &issuer); err != nil {
			err = rows.Close()
			if err != nil {
				return nil, fmt.Errorf("closing rows: %w", err)
			}
			return nil, fmt.Errorf("scanning trustline asset: %w", err)
		}
		result[code+":"+issuer] = id
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("closing rows: %w", err)
	}

	// Step 2: If all found, we're done (fast path - most common case)
	if len(result) == len(assets) {
		m.MetricsService.ObserveDBQueryDuration("BatchGetOrCreateIDs", "trustline_assets", time.Since(start).Seconds())
		m.MetricsService.IncDBQuery("BatchGetOrCreateIDs", "trustline_assets")
		return result, nil
	}

	// Step 3: Insert only missing ones
	var newCodes, newIssuers []string
	for i, a := range assets {
		if _, exists := result[a.Code+":"+a.Issuer]; !exists {
			newCodes = append(newCodes, codes[i])
			newIssuers = append(newIssuers, issuers[i])
		}
	}

	const insertQuery = `
		INSERT INTO trustline_assets (code, issuer)
		SELECT * FROM UNNEST($1::text[], $2::text[])
		ON CONFLICT (code, issuer) DO UPDATE SET code = EXCLUDED.code
		RETURNING id, code, issuer
	`

	rows, err = m.DB.QueryxContext(ctx, insertQuery, pq.Array(newCodes), pq.Array(newIssuers))
	if err != nil {
		return nil, fmt.Errorf("inserting trustline assets: %w", err)
	}

	for rows.Next() {
		var id int64
		var code, issuer string
		if err := rows.Scan(&id, &code, &issuer); err != nil {
			return nil, fmt.Errorf("scanning inserted trustline asset: %w", err)
		}
		result[code+":"+issuer] = id
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("closing rows: %w", err)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchGetOrCreateIDs", "trustline_assets", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchGetOrCreateIDs", "trustline_assets")
	return result, nil
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
