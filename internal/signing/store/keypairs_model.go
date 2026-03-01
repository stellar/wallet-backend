package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/stellar/wallet-backend/internal/db"
)

var (
	ErrKeypairNotFound        = errors.New("keypair not found")
	ErrPublicKeyAlreadyExists = errors.New("public key already exists")
)

type KeypairModel struct {
	DB db.ConnectionPool
}

var _ KeypairStore = (*KeypairModel)(nil)

func (k *KeypairModel) Insert(ctx context.Context, publicKey string, encryptedPrivateKey []byte) error {
	const query = `
		INSERT INTO keypairs (public_key, encrypted_private_key) VALUES ($1, $2)
	`
	_, err := k.DB.Pool().Exec(ctx, query, publicKey, encryptedPrivateKey)
	if err != nil {
		var pgErr *pgconn.PgError
		if ok := errors.As(err, &pgErr); ok && pgErr.ConstraintName == "keypairs_pkey" {
			return ErrPublicKeyAlreadyExists
		}
		return fmt.Errorf("inserting keypair for public key %s: %w", publicKey, err)
	}

	return nil
}

func (k *KeypairModel) GetByPublicKey(ctx context.Context, publicKey string) (*Keypair, error) {
	const query = `
		SELECT
			public_key,
			encrypted_private_key,
			created_at,
			updated_at
		FROM
			keypairs
		WHERE
			public_key = $1
	`
	kp, err := db.QueryOne[Keypair](ctx, k.DB.Pool(), query, publicKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrKeypairNotFound
		}
		return nil, fmt.Errorf("getting keypair for public key %s: %w", publicKey, err)
	}

	return kp, nil
}

func NewKeypairModel(dbConnectionPool db.ConnectionPool) *KeypairModel {
	return &KeypairModel{DB: dbConnectionPool}
}
