package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"

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
	_, err := k.DB.ExecContext(ctx, query, publicKey, encryptedPrivateKey)
	if err != nil {
		// Check pgx/v5 error first (pgconn.PgError.ConstraintName differs from pq.Error.Constraint)
		var pgxError *pgconn.PgError
		if ok := errors.As(err, &pgxError); ok && pgxError.ConstraintName == "keypairs_pkey" {
			return ErrPublicKeyAlreadyExists
		}
		// Fallback: lib/pq error during sqlx transition
		var pqError *pq.Error
		if ok := errors.As(err, &pqError); ok && pqError.Constraint == "keypairs_pkey" {
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
	var kp Keypair
	err := k.DB.GetContext(ctx, &kp, query, publicKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrKeypairNotFound
		}
		return nil, fmt.Errorf("getting keypair for public key %s: %w", publicKey, err)
	}

	return &kp, nil
}

func NewKeypairModel(dbConnectionPool db.ConnectionPool) *KeypairModel {
	return &KeypairModel{DB: dbConnectionPool}
}
