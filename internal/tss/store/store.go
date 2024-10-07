package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/tss"
)

type Store interface {
	GetTransaction(ctx context.Context, hash string) (Transaction, error)
	UpsertTransaction(ctx context.Context, WebhookURL string, txHash string, txXDR string, status tss.RPCTXStatus) error
	UpsertTry(ctx context.Context, transactionHash string, feeBumpTxHash string, feeBumpTxXDR string, status tss.RPCTXStatus, code tss.RPCTXCode, resultXDR string) error
	GetTry(ctx context.Context, hash string) (Try, error)
	GetTryByXDR(ctx context.Context, xdr string) (Try, error)
	GetTransactionsWithStatus(ctx context.Context, status tss.RPCTXStatus) ([]Transaction, error)
	GetLatestTry(ctx context.Context, txHash string) (Try, error)
}

var _ Store = (*store)(nil)

type store struct {
	DB db.ConnectionPool
}

type Transaction struct {
	Hash         string       `db:"transaction_hash"`
	XDR          string       `db:"transaction_xdr"`
	WebhookURL   string       `db:"webhook_url"`
	Status       string       `db:"current_status"`
	CreatedAt    time.Time    `db:"created_at"`
	UpdatedAt    time.Time    `db:"updated_at"`
	ClaimedUntil sql.NullTime `db:"claimed_until"`
}

type Try struct {
	Hash       string    `db:"try_transaction_hash"`
	OrigTxHash string    `db:"original_transaction_hash"`
	XDR        string    `db:"try_transaction_xdr"`
	Status     string    `db:"status"`
	Code       int32     `db:"code"`
	ResultXDR  string    `db:"result_xdr"`
	CreatedAt  time.Time `db:"updated_at"`
}

func NewStore(db db.ConnectionPool) (Store, error) {
	if db == nil {
		return nil, fmt.Errorf("db cannot be nil")
	}
	return &store{
		DB: db,
	}, nil
}

func (s *store) UpsertTransaction(ctx context.Context, webhookURL string, txHash string, txXDR string, status tss.RPCTXStatus) error {
	const q = `
	INSERT INTO 
		tss_transactions (transaction_hash, transaction_xdr, webhook_url, current_status)
	VALUES
		($1, $2, $3, $4)
	ON CONFLICT (transaction_hash) 
	DO UPDATE SET 
		transaction_xdr = EXCLUDED.transaction_xdr,
		webhook_url = EXCLUDED.webhook_url,
    	current_status = EXCLUDED.current_status,
    	updated_at = NOW();
	`
	_, err := s.DB.ExecContext(ctx, q, txHash, txXDR, webhookURL, status.Status())
	if err != nil {
		return fmt.Errorf("inserting/updatig tss transaction: %w", err)
	}
	return nil
}

func (s *store) UpsertTry(ctx context.Context, txHash string, feeBumpTxHash string, feeBumpTxXDR string, status tss.RPCTXStatus, code tss.RPCTXCode, resultXDR string) error {
	const q = `
	INSERT INTO 
		tss_transaction_submission_tries (original_transaction_hash, try_transaction_hash, try_transaction_xdr, status, code, result_xdr)
	VALUES
		($1, $2, $3, $4, $5, $6)
	ON CONFLICT (try_transaction_hash) 
	DO UPDATE SET 
		original_transaction_hash = EXCLUDED.original_transaction_hash,
		try_transaction_xdr = EXCLUDED.try_transaction_xdr,
    	status = EXCLUDED.status,
		code = EXCLUDED.code,
		result_xdr = EXCLUDED.result_xdr,
    	updated_at = NOW();
	`
	_, err := s.DB.ExecContext(ctx, q, txHash, feeBumpTxHash, feeBumpTxXDR, status.Status(), code.Code(), resultXDR)
	if err != nil {
		return fmt.Errorf("inserting/updating tss try: %w", err)
	}
	return nil
}

func (s *store) GetTransaction(ctx context.Context, hash string) (Transaction, error) {
	q := `SELECT * from tss_transactions where transaction_hash = $1`
	var transaction Transaction
	err := s.DB.GetContext(ctx, &transaction, q, hash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Transaction{}, nil
		}
		return Transaction{}, fmt.Errorf("getting transaction: %w", err)
	}
	return transaction, nil
}

func (s *store) GetTry(ctx context.Context, hash string) (Try, error) {
	q := `SELECT * from tss_transaction_submission_tries where try_transaction_hash = $1`
	var try Try
	err := s.DB.GetContext(ctx, &try, q, hash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Try{}, nil
		}
		return Try{}, fmt.Errorf("getting try: %w", err)
	}
	return try, nil
}

func (s *store) GetTryByXDR(ctx context.Context, xdr string) (Try, error) {
	q := `SELECT * from tss_transaction_submission_tries where  try_transaction_xdr = $1`
	var try Try
	err := s.DB.GetContext(ctx, &try, q, xdr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Try{}, nil
		}
		return Try{}, fmt.Errorf("getting try: %w", err)
	}
	return try, nil
}

func (s *store) GetTransactionsWithStatus(ctx context.Context, status tss.RPCTXStatus) ([]Transaction, error) {
	q := `SELECT * from tss_transactions where current_status = $1`
	var transactions []Transaction
	err := s.DB.SelectContext(ctx, &transactions, q, status.Status())
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []Transaction{}, nil
		}
		return []Transaction{}, fmt.Errorf("getting transactions: %w", err)
	}
	return transactions, nil
}

func (s *store) GetLatestTry(ctx context.Context, txHash string) (Try, error) {
	q := `SELECT * from tss_transaction_submission_tries where original_transaction_hash = $1 ORDER BY updated_at DESC LIMIT 1`
	var try Try
	err := s.DB.GetContext(ctx, &try, q, txHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Try{}, nil
		}
		return Try{}, fmt.Errorf("getting latest trt: %w", err)
	}
	return try, nil
}
