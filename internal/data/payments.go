package data

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/internal/db"

	"github.com/jmoiron/sqlx"
	"github.com/stellar/go/support/errors"
)

type PaymentModel struct {
	db db.ConnectionPool
}

type Payment struct {
	OperationID     int64
	OperationType   string
	TransactionID   int64
	TransactionHash string
	From            string
	To              string
	SrcAssetCode    string
	SrcAssetIssuer  string
	SrcAmount       int64
	DestAssetCode   string
	DestAssetIssuer string
	DestAmount      int64
	CreatedAt       time.Time
	Memo            string
}

func (m *PaymentModel) GetLatestLedgerSynced(ctx context.Context, cursorName string) (uint32, error) {
	var lastSyncedLedger uint32
	err := m.db.QueryRowxContext(ctx, `SELECT value FROM ingestion_store WHERE key = $1`, cursorName).Scan(&lastSyncedLedger)
	// First run, key does not exist yet
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return lastSyncedLedger, nil
}

func (m *PaymentModel) UpdateLatestLedgerSynced(ctx context.Context, cursorName string, ledger uint32) error {
	const query = `
		INSERT INTO ingestion_store (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value
	`
	_, err := m.db.ExecContext(ctx, query, cursorName, ledger)
	if err != nil {
		return errors.Wrapf(err, "updating last synced ledger to %d", ledger)
	}

	return nil
}

func (m *PaymentModel) BeginTx(ctx context.Context) (*sqlx.Tx, error) {
	tx, err := m.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "beginning transaction")
	}

	return tx, nil
}

func (m *PaymentModel) AddPayment(ctx context.Context, tx *sqlx.Tx, payment Payment) error {
	const query = `
		INSERT INTO ingestion_payments (
			operation_id, operation_type, transaction_id, transaction_hash, from_address, to_address, src_asset_code, src_asset_issuer, src_amount, 
			dest_asset_code, dest_asset_issuer, dest_amount, created_at, memo
		)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
		WHERE EXISTS (
			SELECT 1 FROM accounts WHERE stellar_address IN ($5, $6)
		)
		ON CONFLICT (operation_id) DO UPDATE SET
			operation_type = EXCLUDED.operation_type,
			transaction_id = EXCLUDED.transaction_id,
			transaction_hash = EXCLUDED.transaction_hash,
			from_address = EXCLUDED.from_address,
			to_address = EXCLUDED.to_address,
			src_asset_code = EXCLUDED.src_asset_code,
			src_asset_issuer = EXCLUDED.src_asset_issuer,
			src_amount = EXCLUDED.src_amount,
			dest_asset_code = EXCLUDED.dest_asset_code,
			dest_asset_issuer = EXCLUDED.dest_asset_issuer,
			dest_amount = EXCLUDED.dest_amount,
			created_at = EXCLUDED.created_at,
			memo = EXCLUDED.memo
		;
	`
	_, err := tx.ExecContext(ctx, query, payment.OperationID, payment.OperationType, payment.TransactionID, payment.TransactionHash, payment.From, payment.To, payment.SrcAssetCode, payment.SrcAssetIssuer, payment.SrcAmount,
		payment.DestAssetCode, payment.DestAssetIssuer, payment.DestAmount, payment.CreatedAt, payment.Memo)
	if err != nil {
		return errors.Wrap(err, "inserting payment")
	}

	return nil
}

func (m *PaymentModel) SubscribeAddress(ctx context.Context, address string) error {
	const query = `INSERT INTO accounts (stellar_address) VALUES ($1) ON CONFLICT DO NOTHING`
	_, err := m.db.ExecContext(ctx, query, address)
	if err != nil {
		return fmt.Errorf("subscribing address %s to payments tracking: %w", address, err)
	}

	return nil
}

func (m *PaymentModel) UnsubscribeAddress(ctx context.Context, address string) error {
	const query = `DELETE FROM accounts WHERE stellar_address = $1`
	_, err := m.db.ExecContext(ctx, query, address)
	if err != nil {
		return fmt.Errorf("unsubscribing address %s to payments tracking: %w", address, err)
	}

	return nil
}
