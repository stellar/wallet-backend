package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/wallet-backend/internal/db"
)

type PaymentModel struct {
	DB db.ConnectionPool
}

type Payment struct {
	OperationID     string    `db:"operation_id" json:"operationId"`
	OperationType   string    `db:"operation_type" json:"operationType"`
	TransactionID   string    `db:"transaction_id" json:"transactionId"`
	TransactionHash string    `db:"transaction_hash" json:"transactionHash"`
	FromAddress     string    `db:"from_address" json:"fromAddress"`
	ToAddress       string    `db:"to_address" json:"toAddress"`
	SrcAssetCode    string    `db:"src_asset_code" json:"srcAssetCode"`
	SrcAssetIssuer  string    `db:"src_asset_issuer" json:"srcAssetIssuer"`
	SrcAssetType    string    `db:"src_asset_type" json:"srcAssetType"`
	SrcAmount       int64     `db:"src_amount" json:"srcAmount"`
	DestAssetCode   string    `db:"dest_asset_code" json:"destAssetCode"`
	DestAssetIssuer string    `db:"dest_asset_issuer" json:"destAssetIssuer"`
	DestAssetType   string    `db:"dest_asset_type" json:"destAssetType"`
	DestAmount      int64     `db:"dest_amount" json:"destAmount"`
	CreatedAt       time.Time `db:"created_at" json:"createdAt"`
	Memo            *string   `db:"memo" json:"memo"`
	MemoType        string    `db:"memo_type" json:"memoType"`
}

func (m *PaymentModel) GetLatestLedgerSynced(ctx context.Context, cursorName string) (uint32, error) {
	var lastSyncedLedger uint32
	err := m.DB.GetContext(ctx, &lastSyncedLedger, `SELECT value FROM ingest_store WHERE key = $1`, cursorName)
	// First run, key does not exist yet
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("getting latest ledger synced for cursor %s: %w", cursorName, err)
	}

	return lastSyncedLedger, nil
}

func (m *PaymentModel) UpdateLatestLedgerSynced(ctx context.Context, cursorName string, ledger uint32) error {
	const query = `
		INSERT INTO ingest_store (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value
	`
	_, err := m.DB.ExecContext(ctx, query, cursorName, ledger)
	if err != nil {
		return fmt.Errorf("updating last synced ledger to %d: %w", ledger, err)
	}

	return nil
}

func (m *PaymentModel) AddPayment(ctx context.Context, tx db.Transaction, payment Payment) error {
	const query = `
		INSERT INTO ingest_payments (
			operation_id, operation_type, transaction_id, transaction_hash, from_address, to_address, src_asset_code, src_asset_issuer, src_asset_type, src_amount, 
			dest_asset_code, dest_asset_issuer, dest_asset_type, dest_amount, created_at, memo, memo_type
		)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
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
			src_asset_type = EXCLUDED.src_asset_type,
			src_amount = EXCLUDED.src_amount,
			dest_asset_code = EXCLUDED.dest_asset_code,
			dest_asset_issuer = EXCLUDED.dest_asset_issuer,
			dest_asset_type = EXCLUDED.dest_asset_type,
			dest_amount = EXCLUDED.dest_amount,
			created_at = EXCLUDED.created_at,
			memo = EXCLUDED.memo,
			memo_type = EXCLUDED.memo_type
		;
	`
	_, err := tx.ExecContext(ctx, query, payment.OperationID, payment.OperationType, payment.TransactionID, payment.TransactionHash, payment.FromAddress, payment.ToAddress, payment.SrcAssetCode, payment.SrcAssetIssuer, payment.SrcAssetType, payment.SrcAmount,
		payment.DestAssetCode, payment.DestAssetIssuer, payment.DestAssetType, payment.DestAmount, payment.CreatedAt, payment.Memo, payment.MemoType)
	if err != nil {
		return fmt.Errorf("inserting payment: %w", err)
	}

	return nil
}

func (m *PaymentModel) GetPaymentsPaginated(ctx context.Context, address string, beforeID, afterID string, sort SortOrder, limit int) ([]Payment, bool, bool, error) {
	if !sort.IsValid() {
		return nil, false, false, fmt.Errorf("invalid sort value: %s", sort)
	}

	if beforeID != "" && afterID != "" {
		return nil, false, false, errors.New("at most one cursor may be provided, got afterId and beforeId")
	}

	const filteredSetCTE = `
		WITH filtered_set AS (
			SELECT * FROM ingest_payments WHERE :address = '' OR :address IN (from_address, to_address)
		)
	`

	var selectQ string
	if beforeID != "" && sort == DESC {
		selectQ = "SELECT * FROM (SELECT * FROM filtered_set WHERE operation_id > :before_id ORDER BY operation_id ASC LIMIT :limit) AS reverse_set ORDER BY operation_id DESC"
	} else if beforeID != "" && sort == ASC {
		selectQ = "SELECT * FROM (SELECT * FROM filtered_set WHERE operation_id < :before_id ORDER BY operation_id DESC LIMIT :limit) AS reverse_set ORDER BY operation_id ASC"
	} else if afterID != "" && sort == DESC {
		selectQ = "SELECT * FROM filtered_set WHERE operation_id < :after_id ORDER BY operation_id DESC LIMIT :limit"
	} else if afterID != "" && sort == ASC {
		selectQ = "SELECT * FROM filtered_set WHERE operation_id > :after_id ORDER BY operation_id ASC LIMIT :limit"
	} else if sort == ASC {
		selectQ = "SELECT * FROM filtered_set ORDER BY operation_id ASC LIMIT :limit"
	} else {
		selectQ = "SELECT * FROM filtered_set ORDER BY operation_id DESC LIMIT :limit"
	}

	argumentsMap := map[string]interface{}{
		"address":   address,
		"limit":     limit,
		"before_id": beforeID,
		"after_id":  afterID,
	}

	payments := make([]Payment, 0)
	query := fmt.Sprintf("%s %s", filteredSetCTE, selectQ)
	query, args, err := PrepareNamedQuery(ctx, m.DB, query, argumentsMap)
	if err != nil {
		return nil, false, false, fmt.Errorf("preparing named query: %w", err)
	}
	err = m.DB.SelectContext(ctx, &payments, query, args...)
	if err != nil {
		return nil, false, false, fmt.Errorf("fetching payments: %w", err)
	}

	prevExists, nextExists, err := m.existsPrevNext(ctx, filteredSetCTE, address, sort, payments)
	if err != nil {
		return nil, false, false, fmt.Errorf("checking prev and next pages: %w", err)
	}

	return payments, prevExists, nextExists, nil
}

func (m *PaymentModel) existsPrevNext(ctx context.Context, filteredSetCTE string, address string, sort SortOrder, payments []Payment) (bool, bool, error) {
	if len(payments) == 0 {
		return false, false, nil
	}

	firstElementID := FirstPaymentOperationID(payments)
	lastElementID := LastPaymentOperationID(payments)

	query := fmt.Sprintf(`
		%s
		SELECT
			EXISTS(
				SELECT 1 FROM filtered_set WHERE CASE WHEN :sort = 'ASC' THEN operation_id < :first_element_id WHEN :sort = 'DESC' THEN operation_id > :first_element_id END LIMIT 1
			) AS prev_exists,
			EXISTS(
				SELECT 1 FROM filtered_set WHERE CASE WHEN :sort = 'ASC' THEN operation_id > :last_element_id WHEN :sort = 'DESC' THEN operation_id < :last_element_id END LIMIT 1
			) AS next_exists
	`, filteredSetCTE)

	argumentsMap := map[string]interface{}{
		"address":          address,
		"first_element_id": firstElementID,
		"last_element_id":  lastElementID,
		"sort":             sort,
	}

	query, args, err := PrepareNamedQuery(ctx, m.DB, query, argumentsMap)
	if err != nil {
		return false, false, fmt.Errorf("preparing named query: %w", err)
	}

	var prevExists, nextExists bool
	err = m.DB.QueryRowxContext(ctx, query, args...).Scan(&prevExists, &nextExists)
	if err != nil {
		return false, false, fmt.Errorf("fetching prev and next exists: %w", err)
	}

	return prevExists, nextExists, nil
}

func FirstPaymentOperationID(payments []Payment) string {
	if len(payments) > 0 {
		return payments[0].OperationID
	}
	return ""
}

func LastPaymentOperationID(payments []Payment) string {
	len := len(payments)
	if len > 0 {
		return payments[len-1].OperationID
	}
	return ""
}
