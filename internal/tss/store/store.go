package store

import (
	"context"
	"fmt"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/tss"
)

type store struct {
	DB  db.ConnectionPool
	ctx context.Context
}

func NewStore(ctx context.Context, db db.ConnectionPool) Store {
	return &store{
		DB:  db,
		ctx: ctx,
	}
}

func (s *store) UpsertTransaction(webhookURL string, txHash string, txXDR string, status tss.RPCTXStatus) error {
	const q = `
	INSERT INTO 
		tss_transactions (transaction_hash, transaction_xdr, webhook_url, current_status)
	VALUES
		($1, $2, $3, $4)
	ON CONFLICT (transaction_hash) 
	DO UPDATE SET 
		transaction_xdr = $2,
		webhook_url = $3,
    	current_status = $4,
    	updated_at = NOW();
	`
	_, err := s.DB.ExecContext(s.ctx, q, txHash, txXDR, webhookURL, string(status))
	if err != nil {
		return fmt.Errorf("inserting/updatig tss transaction: %w", err)
	}
	return nil
}

func (s *store) UpsertTry(txHash string, feeBumpTxHash string, feeBumpTxXDR string, status tss.RPCTXCode) error {
	const q = `
	INSERT INTO 
		tss_transaction_submission_tries (original_transaction_hash, try_transaction_hash, try_transaction_xdr, status)
	VALUES
		($1, $2, $3, $4)
	ON CONFLICT (try_transaction_hash) 
	DO UPDATE SET 
		original_transaction_hash = $1,
		try_transaction_xdr = $3,
    	status = $4,
    	updated_at = NOW();
	`
	var st int
	// if this value is set, it takes precedence over the code from RPC
	if status.OtherCodes != tss.NoCode {
		st = int(status.OtherCodes)
	} else {
		st = int(status.TxResultCode)
	}
	_, err := s.DB.ExecContext(s.ctx, q, txHash, feeBumpTxHash, feeBumpTxXDR, st)
	if err != nil {
		return fmt.Errorf("inserting/updating tss try: %w", err)
	}
	return nil
}
