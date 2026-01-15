// Package data provides data access layer for trustline asset operations.
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/db"
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
	// BatchCopy performs bulk insert using COPY protocol for speed.
	// Callers must ensure no duplicates exist before calling.
	BatchCopy(ctx context.Context, dbTx pgx.Tx, assets []TrustlineAsset) error
}

// TrustlineAssetModel implements TrustlineAssetModelInterface.
type TrustlineAssetModel struct {
	DB             db.ConnectionPool
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

// BatchCopy performs bulk insert using COPY protocol for speed.
// Callers must ensure no duplicates exist before calling.
// Assets must have their ID field set via DeterministicAssetID before calling.
func (m *TrustlineAssetModel) BatchCopy(ctx context.Context, dbTx pgx.Tx, assets []TrustlineAsset) error {
	if len(assets) == 0 {
		return nil
	}

	start := time.Now()

	rows := make([][]any, len(assets))
	for i, a := range assets {
		rows[i] = []any{a.ID, a.Code, a.Issuer}
	}

	copyCount, err := dbTx.CopyFrom(
		ctx,
		pgx.Identifier{"trustline_assets"},
		[]string{"id", "code", "issuer"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("batch inserting trustline assets via COPY: %w", err)
	}

	if int(copyCount) != len(rows) {
		return fmt.Errorf("expected %d rows copied, got %d", len(rows), copyCount)
	}

	m.MetricsService.ObserveDBQueryDuration("BatchCopy", "trustline_assets", time.Since(start).Seconds())
	m.MetricsService.IncDBQuery("BatchCopy", "trustline_assets")
	return nil
}
