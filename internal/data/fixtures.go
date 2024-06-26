package data

import (
	"context"
	"testing"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stretchr/testify/require"
)

func InsertTestPayments(t *testing.T, ctx context.Context, payments []Payment, connectionPool db.ConnectionPool) {
	t.Helper()

	const query = `INSERT INTO ingest_payments (operation_id, operation_type, transaction_id, transaction_hash, from_address, to_address, src_asset_code, src_asset_issuer, src_amount, dest_asset_code, dest_asset_issuer, dest_amount, created_at, memo) VALUES (:operation_id, :operation_type, :transaction_id, :transaction_hash, :from_address, :to_address, :src_asset_code, :src_asset_issuer, :src_amount, :dest_asset_code, :dest_asset_issuer, :dest_amount, :created_at, :memo);`
	_, err := connectionPool.NamedExecContext(ctx, query, payments)
	require.NoError(t, err)
}
