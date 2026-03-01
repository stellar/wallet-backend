package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5"
)

type ChannelAccount struct {
	PublicKey           string         `db:"public_key"`
	EncryptedPrivateKey string         `db:"encrypted_private_key"`
	UpdatedAt           time.Time      `db:"updated_at"`
	CreatedAt           time.Time      `db:"created_at"`
	LockedAt            sql.NullTime   `db:"locked_at"`
	LockedUntil         sql.NullTime   `db:"locked_until"`
	LockedTxHash        sql.NullString `db:"locked_tx_hash"`
}

type ChannelAccountStore interface {
	GetAndLockIdleChannelAccount(ctx context.Context, lockedUntil time.Duration) (*ChannelAccount, error)
	Get(ctx context.Context, publicKey string) (*ChannelAccount, error)
	GetAllByPublicKey(ctx context.Context, publicKeys ...string) ([]*ChannelAccount, error)
	AssignTxToChannelAccount(ctx context.Context, publicKey string, txHash string) error
	UnassignTxAndUnlockChannelAccounts(ctx context.Context, pgxTx pgx.Tx, txHashes ...string) (int64, error)
	BatchInsert(ctx context.Context, channelAccounts []*ChannelAccount) error
	GetAllInTx(ctx context.Context, pgxTx pgx.Tx, limit int) ([]*ChannelAccount, error)
	DeleteInTx(ctx context.Context, pgxTx pgx.Tx, publicKeys ...string) (int64, error)
	Count(ctx context.Context) (int64, error)
}

type Keypair struct {
	PublicKey           string    `db:"public_key"`
	EncryptedPrivateKey []byte    `db:"encrypted_private_key"`
	CreatedAt           time.Time `db:"created_at"`
	UpdatedAt           time.Time `db:"updated_at"`
}

type KeypairStore interface {
	GetByPublicKey(ctx context.Context, publicKey string) (*Keypair, error)
	Insert(ctx context.Context, publicKey string, encryptedPrivateKey []byte) error
}
