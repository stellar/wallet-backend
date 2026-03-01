// Package data provides data access layer for trustline asset operations.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/metrics"
)

// Note: pq is still used by BatchGetByIDs for sqlx compatibility

// assetNamespace is a custom namespace UUID derived deterministically from the
// DNS namespace UUID and the "trustline_assets" table name. This ensures a
// stable, reproducible namespace for generating asset IDs.
var assetNamespace = uuid.NewSHA1(uuid.NameSpaceDNS, []byte("trustline_assets"))

// DeterministicAssetID computes a deterministic UUID for a trustline asset
// using UUID v5 (SHA-1) of "CODE:ISSUER". This enables streaming checkpoint processing
// without needing DB roundtrips to get auto-generated IDs.
func DeterministicAssetID(code, issuer string) uuid.UUID {
	return uuid.NewSHA1(assetNamespace, []byte(code+":"+issuer))
}

// TrustlineAssetModelInterface defines the interface for trustline asset operations.
type TrustlineAssetModelInterface interface {
	// BatchInsert inserts multiple trustline assets with pre-computed IDs.
	// Uses INSERT ... ON CONFLICT (code, issuer) DO NOTHING for idempotent operations.
	BatchInsert(ctx context.Context, dbTx pgx.Tx, assets []TrustlineAsset) error
}

// TrustlineAssetModel implements TrustlineAssetModelInterface.
type TrustlineAssetModel struct {
	DB             *pgxpool.Pool
	MetricsService metrics.MetricsService
}

var _ TrustlineAssetModelInterface = (*TrustlineAssetModel)(nil)

// TrustlineAsset represents a classic Stellar trustline asset.
type TrustlineAsset struct {
	ID        uuid.UUID `db:"id" json:"id"`
	Code      string    `db:"code" json:"code"`
	Issuer    string    `db:"issuer" json:"issuer"`
	CreatedAt time.Time `db:"created_at" json:"createdAt"`
}

// AssetKey returns the "CODE:ISSUER" format string for this asset.
func (a *TrustlineAsset) AssetKey() string {
	return fmt.Sprintf("%s:%s", a.Code, a.Issuer)
}

// BatchInsert inserts multiple trustline assets with pre-computed deterministic IDs.
// Uses INSERT ... ON CONFLICT (code, issuer) DO NOTHING for idempotent operations.
// Assets must have their ID field set via DeterministicAssetID before calling.
func (m *TrustlineAssetModel) BatchInsert(ctx context.Context, dbTx pgx.Tx, assets []TrustlineAsset) error {
	if len(assets) == 0 {
		return nil
	}

	ids := make([]uuid.UUID, len(assets))
	codes := make([]string, len(assets))
	issuers := make([]string, len(assets))
	for i, a := range assets {
		ids[i] = a.ID
		codes[i] = a.Code
		issuers[i] = a.Issuer
	}

	const query = `
		INSERT INTO trustline_assets (id, code, issuer)
		SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::text[])
		ON CONFLICT (code, issuer) DO NOTHING
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
