package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

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

	var channelAccount ChannelAccount
	err := ca.DB.GetContext(ctx, &channelAccount, query)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoIdleChannelAccountAvailable
		}

		return nil, fmt.Errorf("getting idle channel account: %w", err)
	}
	return &channelAccount, nil
}

func (ca *ChannelAccountModel) Get(ctx context.Context, sqlExec db.SQLExecuter, publicKey string) (*ChannelAccount, error) {
	const query = `SELECT * FROM channel_accounts WHERE public_key = $1`

	var channelAccount ChannelAccount
	err := sqlExec.GetContext(ctx, &channelAccount, query, publicKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrChannelAccountNotFound
		}
		return nil, fmt.Errorf("getting channel account %s: %w", publicKey, err)
	}

	return &channelAccount, nil
}

func (ca *ChannelAccountModel) GetAllByPublicKey(ctx context.Context, sqlExec db.SQLExecuter, publicKeys ...string) ([]*ChannelAccount, error) {
	const query = `SELECT * FROM channel_accounts WHERE public_key = ANY($1)`

	var channelAccounts []*ChannelAccount
	err := sqlExec.SelectContext(ctx, &channelAccounts, query, pq.Array(publicKeys))
	if err != nil {
		return nil, fmt.Errorf("getting channel accounts %v: %w", publicKeys, err)
	}

	return channelAccounts, nil
}

func (ca *ChannelAccountModel) AssignTxToChannelAccount(ctx context.Context, publicKey string, txHash string) error {
	const query = `UPDATE channel_accounts SET locked_tx_hash = $1 WHERE public_key = $2`
	_, err := ca.DB.ExecContext(ctx, query, txHash, publicKey)
	if err != nil {
		return fmt.Errorf("assigning channel account: %w", err)
	}
	return nil
}

func (ca *ChannelAccountModel) UnassignTxAndUnlockChannelAccounts(ctx context.Context, sqlExec db.SQLExecuter, txHashes ...string) (int64, error) {
	if sqlExec == nil {
		sqlExec = ca.DB
	}

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
	res, err := sqlExec.ExecContext(ctx, query, pq.Array(txHashes))
	if err != nil {
		return 0, fmt.Errorf("unlocking channel accounts %v: %w", txHashes, err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("getting rows affected: %w", err)
	}

	return rowsAffected, nil
}

func (ca *ChannelAccountModel) BatchInsert(ctx context.Context, sqlExec db.SQLExecuter, channelAccounts []*ChannelAccount) error {
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

	_, err := sqlExec.ExecContext(ctx, q, pq.Array(publicKeys), pq.Array(encryptedPrivateKeys))
	if err != nil {
		return fmt.Errorf("inserting channel accounts: %w", err)
	}

	return nil
}

func (ca *ChannelAccountModel) Count(ctx context.Context) (int64, error) {
	query := `
		SELECT
			COUNT(*)
		FROM
			channel_accounts 
	`

	var count int64
	err := ca.DB.GetContext(ctx, &count, query)
	if err != nil {
		return 0, fmt.Errorf("counting channel accounts: %w", err)
	}

	return count, nil
}

func NewChannelAccountModel(dbConnectionPool db.ConnectionPool) *ChannelAccountModel {
	return &ChannelAccountModel{DB: dbConnectionPool}
}
