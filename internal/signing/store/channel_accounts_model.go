package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/db"
)

var (
	ErrNoIdleChannelAccountAvailable = errors.New("no idle channel account available")
	ErrNoChannelAccountConfigured    = errors.New("no channel accounts")
	ErrChannelAccountNotFound        = errors.New("channel account not found")
)

type ChannelAccountModel struct {
	DB db.ConnectionPool
}

var _ ChannelAccountStore = (*ChannelAccountModel)(nil)

func (ca *ChannelAccountModel) GetAndLockIdleChannelAccount(ctx context.Context, lockedUntil time.Duration) (*ChannelAccount, error) {
	query := fmt.Sprintf(`
		UPDATE channel_accounts
		SET
			locked_tx_hash = NULL,
			locked_at = NOW(),
			locked_until = NOW() + INTERVAL '%d seconds'
		WHERE public_key = (
			SELECT
				public_key
			FROM channel_accounts
			WHERE
				locked_until IS NULL
				OR locked_until < NOW()
			ORDER BY random()
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING *;
	`, int64(lockedUntil.Seconds()))

	channelAccount, err := db.QueryOne[ChannelAccount](ctx, ca.DB.Pool(), query)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoIdleChannelAccountAvailable
		}

		return nil, fmt.Errorf("getting idle channel account: %w", err)
	}
	return channelAccount, nil
}

func (ca *ChannelAccountModel) Get(ctx context.Context, publicKey string) (*ChannelAccount, error) {
	const query = `SELECT * FROM channel_accounts WHERE public_key = $1`

	channelAccount, err := db.QueryOne[ChannelAccount](ctx, ca.DB.Pool(), query, publicKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrChannelAccountNotFound
		}
		return nil, fmt.Errorf("getting channel account %s: %w", publicKey, err)
	}

	return channelAccount, nil
}

func (ca *ChannelAccountModel) GetAllByPublicKey(ctx context.Context, publicKeys ...string) ([]*ChannelAccount, error) {
	const query = `SELECT * FROM channel_accounts WHERE public_key = ANY($1)`

	channelAccounts, err := db.QueryAll[ChannelAccount](ctx, ca.DB.Pool(), query, publicKeys)
	if err != nil {
		return nil, fmt.Errorf("getting channel accounts %v: %w", publicKeys, err)
	}

	return channelAccounts, nil
}

func (ca *ChannelAccountModel) AssignTxToChannelAccount(ctx context.Context, publicKey string, txHash string) error {
	const query = `UPDATE channel_accounts SET locked_tx_hash = $1 WHERE public_key = $2`
	_, err := ca.DB.Pool().Exec(ctx, query, txHash, publicKey)
	if err != nil {
		return fmt.Errorf("assigning channel account: %w", err)
	}
	return nil
}

func (ca *ChannelAccountModel) UnassignTxAndUnlockChannelAccounts(ctx context.Context, pgxTx pgx.Tx, txHashes ...string) (int64, error) {
	if len(txHashes) == 0 {
		return 0, errors.New("txHashes cannot be empty")
	}

	const query = `
		UPDATE channel_accounts
		SET
			locked_tx_hash = NULL,
			locked_at = NULL,
			locked_until = NULL
		WHERE
			locked_tx_hash = ANY($1)
	`
	result, err := pgxTx.Exec(ctx, query, txHashes)
	if err != nil {
		return 0, fmt.Errorf("unlocking channel accounts %v: %w", txHashes, err)
	}

	return result.RowsAffected(), nil
}

func (ca *ChannelAccountModel) BatchInsert(ctx context.Context, channelAccounts []*ChannelAccount) error {
	if len(channelAccounts) == 0 {
		return nil
	}

	publicKeys := make([]string, len(channelAccounts))
	encryptedPrivateKeys := make([]string, len(channelAccounts))
	for i, ca := range channelAccounts {
		if ca.PublicKey == "" {
			return fmt.Errorf("public key cannot be empty")
		}
		if ca.EncryptedPrivateKey == "" {
			return fmt.Errorf("private key cannot be empty")
		}

		publicKeys[i] = ca.PublicKey
		encryptedPrivateKeys[i] = ca.EncryptedPrivateKey
	}

	const q = `
		INSERT INTO
			channel_accounts (public_key, encrypted_private_key)
		SELECT *
			FROM UNNEST($1::text[], $2::text[])
	`

	_, err := ca.DB.Pool().Exec(ctx, q, publicKeys, encryptedPrivateKeys)
	if err != nil {
		return fmt.Errorf("inserting channel accounts: %w", err)
	}

	return nil
}

func (ca *ChannelAccountModel) GetAllInTx(ctx context.Context, pgxTx pgx.Tx, limit int) ([]*ChannelAccount, error) {
	query := `
		SELECT * FROM channel_accounts
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`

	rows, err := pgxTx.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("getting all channel accounts: %w", err)
	}
	channelAccounts, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[ChannelAccount])
	if err != nil {
		return nil, fmt.Errorf("collecting channel accounts: %w", err)
	}

	return channelAccounts, nil
}

func (ca *ChannelAccountModel) DeleteInTx(ctx context.Context, pgxTx pgx.Tx, publicKeys ...string) (int64, error) {
	query := `DELETE FROM channel_accounts WHERE public_key = ANY($1)`

	result, err := pgxTx.Exec(ctx, query, publicKeys)
	if err != nil {
		return 0, fmt.Errorf("deleting channel accounts %v: %w", publicKeys, err)
	}

	return result.RowsAffected(), nil
}

func (ca *ChannelAccountModel) Count(ctx context.Context) (int64, error) {
	query := `
		SELECT
			COUNT(*)
		FROM
			channel_accounts
	`

	var count int64
	err := ca.DB.Pool().QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting channel accounts: %w", err)
	}

	return count, nil
}

func NewChannelAccountModel(dbConnectionPool db.ConnectionPool) *ChannelAccountModel {
	return &ChannelAccountModel{DB: dbConnectionPool}
}
