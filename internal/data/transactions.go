package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

type TransactionModel struct {
	DB             db.ConnectionPool
	MetricsService metrics.MetricsService
}

func (m *TransactionModel) GetByHash(ctx context.Context, hash string, columns string) (*types.Transaction, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "", "to_id")
	query := fmt.Sprintf(`SELECT %s FROM transactions WHERE hash = $1`, columns)
	var transaction types.Transaction
	start := time.Now()
	err := m.DB.GetContext(ctx, &transaction, query, hash)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetByHash", "transactions", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetByHash", "transactions", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting transaction %s: %w", hash, err)
	}
	m.MetricsService.IncDBQuery("GetByHash", "transactions")
	return &transaction, nil
}

func (m *TransactionModel) GetAll(ctx context.Context, columns string, limit *int32, cursor *int64, sortOrder SortOrder) ([]*types.TransactionWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "", "to_id")
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString(fmt.Sprintf(`SELECT %s, to_id as cursor FROM transactions`, columns))

	if cursor != nil {
		if sortOrder == DESC {
			queryBuilder.WriteString(fmt.Sprintf(" WHERE to_id < %d", *cursor))
		} else {
			queryBuilder.WriteString(fmt.Sprintf(" WHERE to_id > %d", *cursor))
		}
	}

	if sortOrder == DESC {
		queryBuilder.WriteString(" ORDER BY to_id DESC")
	} else {
		queryBuilder.WriteString(" ORDER BY to_id ASC")
	}

	if limit != nil {
		queryBuilder.WriteString(fmt.Sprintf(" LIMIT %d", *limit))
	}

	query := queryBuilder.String()
	if sortOrder == DESC {
		query = fmt.Sprintf(`SELECT * FROM (%s) AS transactions ORDER BY cursor ASC`, query)
	}

	var transactions []*types.TransactionWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &transactions, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("GetAll", "transactions", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("GetAll", "transactions", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting transactions: %w", err)
	}
	m.MetricsService.IncDBQuery("GetAll", "transactions")
	return transactions, nil
}

// BatchGetByAccountAddress gets the transactions that are associated with a single account address.
func (m *TransactionModel) BatchGetByAccountAddress(ctx context.Context, accountAddress string, columns string, limit *int32, cursor *int64, orderBy SortOrder) ([]*types.TransactionWithCursor, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "transactions", "to_id")

	// Build paginated query using shared utility
	query, args := buildGetByAccountAddressQuery(paginatedQueryConfig{
		TableName:      "transactions",
		CursorColumn:   "to_id",
		JoinTable:      "transactions_accounts",
		JoinCondition:  "transactions_accounts.tx_to_id = transactions.to_id",
		Columns:        columns,
		AccountAddress: accountAddress,
		Limit:          limit,
		Cursor:         cursor,
		OrderBy:        orderBy,
	})

	var transactions []*types.TransactionWithCursor
	start := time.Now()
	err := m.DB.SelectContext(ctx, &transactions, query, args...)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByAccountAddress", "transactions", duration)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByAccountAddress", "transactions", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting transactions by account address: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByAccountAddress", "transactions")
	return transactions, nil
}

// BatchGetByOperationIDs gets the transactions that are associated with the given operation IDs.
//
// Uses TOID bit masking to derive tx_to_id from operation ID:
//
//	tx_to_id = operation_id &^ 0xFFF
//
// This works because TOID encodes (ledger, tx_order, op_index) where:
//   - Lower 12 bits = operation index (1-4095, or 0 for transaction itself)
//   - Clearing these bits yields the transaction's to_id
//
// See SEP-35: https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0035.md
func (m *TransactionModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string) ([]*types.TransactionWithOperationID, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "transactions", "to_id")
	// Join operations to transactions using TOID encoding:
	// An operation ID's lower 12 bits encode the operation index within the transaction.
	// Masking these bits (id &~ 0xFFF) gives the transaction's to_id.
	query := fmt.Sprintf(`
		SELECT %s, o.id as operation_id
		FROM operations o
		INNER JOIN transactions
		ON (o.id & (~x'FFF'::bigint)) = transactions.to_id
		WHERE o.id = ANY($1)`, columns)
	var transactions []*types.TransactionWithOperationID
	start := time.Now()
	err := m.DB.SelectContext(ctx, &transactions, query, pq.Array(operationIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByOperationIDs", "transactions", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByOperationIDs", "transactions", len(operationIDs))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByOperationIDs", "transactions", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting transactions by operation IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByOperationIDs", "transactions")
	return transactions, nil
}

// BatchGetByStateChangeIDs gets the transactions that are associated with the given state changes
func (m *TransactionModel) BatchGetByStateChangeIDs(ctx context.Context, scToIDs []int64, scOpIDs []int64, scOrders []int64, columns string) ([]*types.TransactionWithStateChangeID, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "transactions", "to_id")

	// Build tuples for the IN clause. Since (to_id, operation_id, state_change_order) is the primary key of state_changes,
	// it will be faster to search on this tuple.
	tuples := make([]string, len(scOrders))
	for i := range scOrders {
		tuples[i] = fmt.Sprintf("(%d, %d, %d)", scToIDs[i], scOpIDs[i], scOrders[i])
	}

	query := fmt.Sprintf(`
		SELECT %s, CONCAT(sc.to_id, '-', sc.operation_id, '-', sc.state_change_order) as state_change_id
		FROM transactions
		INNER JOIN state_changes sc ON transactions.to_id = sc.to_id
		WHERE (sc.to_id, sc.operation_id, sc.state_change_order) IN (%s)
		`, columns, strings.Join(tuples, ", "))

	var transactions []*types.TransactionWithStateChangeID
	start := time.Now()
	err := m.DB.SelectContext(ctx, &transactions, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchGetByStateChangeIDs", "transactions", duration)
	m.MetricsService.ObserveDBBatchSize("BatchGetByStateChangeIDs", "transactions", len(scOrders))
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchGetByStateChangeIDs", "transactions", utils.GetDBErrorType(err))
		return nil, fmt.Errorf("getting transactions by state change IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("BatchGetByStateChangeIDs", "transactions")
	return transactions, nil
}

// BatchInsert inserts the transactions and the transactions_accounts links.
// It returns the hashes of the successfully inserted transactions.
func (m *TransactionModel) BatchInsert(
	ctx context.Context,
	sqlExecuter db.SQLExecuter,
	txs []*types.Transaction,
	stellarAddressesByToID map[int64]set.Set[string],
) ([]string, error) {
	if sqlExecuter == nil {
		sqlExecuter = m.DB
	}

	// 1. Flatten the transactions into parallel slices
	hashes := make([]string, len(txs))
	toIDs := make([]int64, len(txs))
	envelopeXDRs := make([]*string, len(txs))
	feesCharged := make([]int64, len(txs))
	resultCodes := make([]string, len(txs))
	metaXDRs := make([]*string, len(txs))
	ledgerNumbers := make([]int, len(txs))
	ledgerCreatedAts := make([]time.Time, len(txs))
	isFeeBumps := make([]bool, len(txs))

	for i, t := range txs {
		hashes[i] = t.Hash
		toIDs[i] = t.ToID
		envelopeXDRs[i] = t.EnvelopeXDR
		feesCharged[i] = t.FeeCharged
		resultCodes[i] = t.ResultCode
		metaXDRs[i] = t.MetaXDR
		ledgerNumbers[i] = int(t.LedgerNumber)
		ledgerCreatedAts[i] = t.LedgerCreatedAt
		isFeeBumps[i] = t.IsFeeBump
	}

	// 2. Build ToID -> LedgerCreatedAt lookup from transactions
	ledgerCreatedAtByToID := make(map[int64]time.Time, len(txs))
	for _, tx := range txs {
		ledgerCreatedAtByToID[tx.ToID] = tx.LedgerCreatedAt
	}

	// 3. Flatten the stellarAddressesByToID into parallel slices
	var txToIDs []int64
	var stellarAddresses []string
	var taLedgerCreatedAts []time.Time
	for toID, addresses := range stellarAddressesByToID {
		ledgerCreatedAt := ledgerCreatedAtByToID[toID]
		for address := range addresses.Iter() {
			txToIDs = append(txToIDs, toID)
			stellarAddresses = append(stellarAddresses, address)
			taLedgerCreatedAts = append(taLedgerCreatedAts, ledgerCreatedAt)
		}
	}

	// Insert transactions and transactions_accounts links with minimal account validation.
	const insertQuery = `
	WITH
	-- Insert transactions
	inserted_transactions AS (
		INSERT INTO transactions
			(hash, to_id, envelope_xdr, fee_charged, result_code, meta_xdr, ledger_number, ledger_created_at, is_fee_bump)
		SELECT
			t.hash, t.to_id, t.envelope_xdr, t.fee_charged, t.result_code, t.meta_xdr, t.ledger_number, t.ledger_created_at, t.is_fee_bump
		FROM (
			SELECT
				UNNEST($1::text[]) AS hash,
				UNNEST($2::bigint[]) AS to_id,
				UNNEST($3::text[]) AS envelope_xdr,
				UNNEST($4::bigint[]) AS fee_charged,
				UNNEST($5::text[]) AS result_code,
				UNNEST($6::text[]) AS meta_xdr,
				UNNEST($7::bigint[]) AS ledger_number,
				UNNEST($8::timestamptz[]) AS ledger_created_at,
				UNNEST($9::boolean[]) AS is_fee_bump
		) t
		ON CONFLICT (ledger_created_at, to_id) DO NOTHING
		RETURNING hash
	),

	-- Insert transactions_accounts links
	inserted_transactions_accounts AS (
		INSERT INTO transactions_accounts
			(ledger_created_at, tx_to_id, account_id)
		SELECT
			ta.ledger_created_at, ta.tx_to_id, ta.account_id
		FROM (
			SELECT
				UNNEST($10::timestamptz[]) AS ledger_created_at,
				UNNEST($11::bigint[]) AS tx_to_id,
				UNNEST($12::text[]) AS account_id
		) ta
		ON CONFLICT DO NOTHING
	)

	-- Return the hashes of successfully inserted transactions
	SELECT hash FROM inserted_transactions;
    `

	start := time.Now()
	var insertedHashes []string
	err := sqlExecuter.SelectContext(ctx, &insertedHashes, insertQuery,
		pq.Array(hashes),
		pq.Array(toIDs),
		pq.Array(envelopeXDRs),
		pq.Array(feesCharged),
		pq.Array(resultCodes),
		pq.Array(metaXDRs),
		pq.Array(ledgerNumbers),
		pq.Array(ledgerCreatedAts),
		pq.Array(isFeeBumps),
		pq.Array(taLedgerCreatedAts),
		pq.Array(txToIDs),
		pq.Array(stellarAddresses),
	)
	duration := time.Since(start).Seconds()
	for _, dbTableName := range []string{"transactions", "transactions_accounts"} {
		m.MetricsService.ObserveDBQueryDuration("BatchInsert", dbTableName, duration)
		if dbTableName == "transactions" {
			m.MetricsService.ObserveDBBatchSize("BatchInsert", dbTableName, len(txs))
		}
		if err == nil {
			m.MetricsService.IncDBQuery("BatchInsert", dbTableName)
		}
	}
	if err != nil {
		for _, dbTableName := range []string{"transactions", "transactions_accounts"} {
			m.MetricsService.IncDBQueryError("BatchInsert", dbTableName, utils.GetDBErrorType(err))
		}
		return nil, fmt.Errorf("batch inserting transactions and transactions_accounts: %w", err)
	}

	return insertedHashes, nil
}

// BatchCopy inserts transactions using pgx's binary COPY protocol.
// Uses pgx.Tx for binary format which is faster than lib/pq's text format.
// Uses native pgtype types for optimal performance (see https://github.com/jackc/pgx/issues/763).
//
// IMPORTANT: Unlike BatchInsert which uses ON CONFLICT DO NOTHING, BatchCopy will FAIL
// if any duplicate records exist. The PostgreSQL COPY protocol does not support conflict
// handling. Callers must ensure no duplicates exist before calling this method, or handle
// the unique constraint violation error appropriately.
func (m *TransactionModel) BatchCopy(
	ctx context.Context,
	pgxTx pgx.Tx,
	txs []*types.Transaction,
	stellarAddressesByToID map[int64]set.Set[string],
) (int, error) {
	if len(txs) == 0 {
		return 0, nil
	}

	start := time.Now()

	// COPY transactions using pgx binary format with native pgtype types
	copyCount, err := pgxTx.CopyFrom(
		ctx,
		pgx.Identifier{"transactions"},
		[]string{"hash", "to_id", "envelope_xdr", "fee_charged", "result_code", "meta_xdr", "ledger_number", "ledger_created_at", "is_fee_bump"},
		pgx.CopyFromSlice(len(txs), func(i int) ([]any, error) {
			tx := txs[i]
			return []any{
				pgtype.Text{String: tx.Hash, Valid: true},
				pgtype.Int8{Int64: tx.ToID, Valid: true},
				pgtypeTextFromPtr(tx.EnvelopeXDR),
				pgtype.Int8{Int64: tx.FeeCharged, Valid: true},
				pgtype.Text{String: tx.ResultCode, Valid: true},
				pgtypeTextFromPtr(tx.MetaXDR),
				pgtype.Int4{Int32: int32(tx.LedgerNumber), Valid: true},
				pgtype.Timestamptz{Time: tx.LedgerCreatedAt, Valid: true},
				pgtype.Bool{Bool: tx.IsFeeBump, Valid: true},
			}, nil
		}),
	)
	if err != nil {
		m.MetricsService.IncDBQueryError("BatchCopy", "transactions", utils.GetDBErrorType(err))
		return 0, fmt.Errorf("pgx CopyFrom transactions: %w", err)
	}
	if int(copyCount) != len(txs) {
		return 0, fmt.Errorf("expected %d rows copied, got %d", len(txs), copyCount)
	}

	// COPY transactions_accounts using pgx binary format with native pgtype types
	if len(stellarAddressesByToID) > 0 {
		// Build ToID -> LedgerCreatedAt lookup from transactions
		ledgerCreatedAtByToID := make(map[int64]time.Time, len(txs))
		for _, tx := range txs {
			ledgerCreatedAtByToID[tx.ToID] = tx.LedgerCreatedAt
		}

		var taRows [][]any
		for toID, addresses := range stellarAddressesByToID {
			ledgerCreatedAt := ledgerCreatedAtByToID[toID]
			ledgerCreatedAtPgtype := pgtype.Timestamptz{Time: ledgerCreatedAt, Valid: true}
			toIDPgtype := pgtype.Int8{Int64: toID, Valid: true}
			for _, addr := range addresses.ToSlice() {
				taRows = append(taRows, []any{
					ledgerCreatedAtPgtype,
					toIDPgtype,
					pgtype.Text{String: addr, Valid: true},
				})
			}
		}

		_, err = pgxTx.CopyFrom(
			ctx,
			pgx.Identifier{"transactions_accounts"},
			[]string{"ledger_created_at", "tx_to_id", "account_id"},
			pgx.CopyFromRows(taRows),
		)
		if err != nil {
			m.MetricsService.IncDBQueryError("BatchCopy", "transactions_accounts", utils.GetDBErrorType(err))
			return 0, fmt.Errorf("pgx CopyFrom transactions_accounts: %w", err)
		}

		m.MetricsService.IncDBQuery("BatchCopy", "transactions_accounts")
	}

	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("BatchCopy", "transactions", duration)
	m.MetricsService.ObserveDBBatchSize("BatchCopy", "transactions", len(txs))
	m.MetricsService.IncDBQuery("BatchCopy", "transactions")

	return len(txs), nil
}

// pgtypeTextFromPtr converts a *string to pgtype.Text for efficient binary COPY.
func pgtypeTextFromPtr(s *string) pgtype.Text {
	if s == nil {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: *s, Valid: true}
}
