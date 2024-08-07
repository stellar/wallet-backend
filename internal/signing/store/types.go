package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/stellar/wallet-backend/internal/db"
)

type ChannelAccount struct {
	PublicKey           string       `db:"public_key"`
	EncryptedPrivateKey string       `db:"encrypted_private_key"`
	UpdatedAt           time.Time    `db:"updated_at"`
	CreatedAt           time.Time    `db:"created_at"`
	LockedAt            sql.NullTime `db:"locked_at"`
	LockedUntil         sql.NullTime `db:"locked_until"`
}

type ChannelAccountStore interface {
	GetIdleChannelAccount(ctx context.Context, lockedUntil time.Duration) (*ChannelAccount, error)
	Get(ctx context.Context, sqlExec db.SQLExecuter, publicKey string) (*ChannelAccount, error)
	GetAllByPublicKey(ctx context.Context, sqlExec db.SQLExecuter, publicKeys ...string) ([]*ChannelAccount, error)
	BatchInsert(ctx context.Context, sqlExec db.SQLExecuter, channelAccounts []*ChannelAccount) error
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
