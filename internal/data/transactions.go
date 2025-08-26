package data

import (
	"context"
	"fmt"
	"strings"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/lib/pq"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "transactions", duration)
	if err != nil {
		return nil, fmt.Errorf("getting transaction %s: %w", hash, err)
	}
	m.MetricsService.IncDBQuery("SELECT", "transactions")
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "transactions", duration)
	if err != nil {
		return nil, fmt.Errorf("getting transactions: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "transactions")
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
		JoinCondition:  "transactions_accounts.tx_hash = transactions.hash",
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
	m.MetricsService.ObserveDBQueryDuration("SELECT", "transactions", duration)
	if err != nil {
		return nil, fmt.Errorf("getting transactions by account address: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "transactions")
	return transactions, nil
}

// BatchGetByOperationIDs gets the transactions that are associated with the given operation IDs.
func (m *TransactionModel) BatchGetByOperationIDs(ctx context.Context, operationIDs []int64, columns string) ([]*types.TransactionWithOperationID, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "transactions", "to_id")
	query := fmt.Sprintf(`
		SELECT %s, o.id as operation_id
		FROM operations o
		INNER JOIN transactions
		ON o.tx_hash = transactions.hash 
		WHERE o.id = ANY($1)`, columns)
	var transactions []*types.TransactionWithOperationID
	start := time.Now()
	err := m.DB.SelectContext(ctx, &transactions, query, pq.Array(operationIDs))
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "transactions", duration)
	if err != nil {
		return nil, fmt.Errorf("getting transactions by operation IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "transactions")
	return transactions, nil
}

// BatchGetByStateChangeIDs gets the transactions that are associated with the given state changes
func (m *TransactionModel) BatchGetByStateChangeIDs(ctx context.Context, scToIDs []int64, scOrders []int64, columns string) ([]*types.TransactionWithStateChangeID, error) {
	columns = prepareColumnsWithID(columns, types.Transaction{}, "transactions", "to_id")

	// Build tuples for the IN clause. Since (to_id, state_change_order) is the primary key of state_changes,
	// it will be faster to search on this tuple.
	tuples := make([]string, len(scOrders))
	for i := range scOrders {
		tuples[i] = fmt.Sprintf("(%d, %d)", scToIDs[i], scOrders[i])
	}

	query := fmt.Sprintf(`
		SELECT %s, CONCAT(sc.to_id, '-', sc.state_change_order) as state_change_id
		FROM transactions
		INNER JOIN state_changes sc ON transactions.hash = sc.tx_hash 
		WHERE (sc.to_id, sc.state_change_order) IN (%s)
		`, columns, strings.Join(tuples, ", "))

	var transactions []*types.TransactionWithStateChangeID
	start := time.Now()
	err := m.DB.SelectContext(ctx, &transactions, query)
	duration := time.Since(start).Seconds()
	m.MetricsService.ObserveDBQueryDuration("SELECT", "transactions", duration)
	if err != nil {
		return nil, fmt.Errorf("getting transactions by state change IDs: %w", err)
	}
	m.MetricsService.IncDBQuery("SELECT", "transactions")
	return transactions, nil
}

// BatchInsert inserts the transactions and the transactions_accounts links.
// It returns the hashes of the successfully inserted transactions.
func (m *TransactionModel) BatchInsert(
	ctx context.Context,
	sqlExecuter db.SQLExecuter,
	txs []types.Transaction,
	stellarAddressesByTxHash map[string]set.Set[string],
) ([]string, error) {
	if sqlExecuter == nil {
		sqlExecuter = m.DB
	}

	// 1. Flatten the transactions into parallel slices
	hashes := make([]string, len(txs))
	toIDs := make([]int64, len(txs))
	envelopeXDRs := make([]string, len(txs))
	resultXDRs := make([]string, len(txs))
	metaXDRs := make([]string, len(txs))
	ledgerNumbers := make([]int, len(txs))
	ledgerCreatedAts := make([]time.Time, len(txs))

	for i, t := range txs {
		hashes[i] = t.Hash
		toIDs[i] = t.ToID
		envelopeXDRs[i] = t.EnvelopeXDR
		resultXDRs[i] = t.ResultXDR
		metaXDRs[i] = t.MetaXDR
		ledgerNumbers[i] = int(t.LedgerNumber)
		ledgerCreatedAts[i] = t.LedgerCreatedAt
	}

	// 2. Flatten the stellarAddressesByTxHash into parallel slices
	var txHashes, stellarAddresses []string
	for txHash, addresses := range stellarAddressesByTxHash {
		for address := range addresses.Iter() {
			txHashes = append(txHashes, txHash)
			stellarAddresses = append(stellarAddresses, address)
		}
	}

	// 3. Single query that inserts only transactions that are connected to at least one existing account.
	// It also inserts the transactions_accounts links.
	const insertQuery = `
	-- STEP 1: Get existing accounts
	WITH existing_accounts AS (
		SELECT stellar_address FROM accounts WHERE stellar_address=ANY($9)
	),

	-- STEP 2: Get transaction hashes to insert (connected to at least one existing account)
    valid_transactions AS (
        SELECT DISTINCT tx_hash
        FROM (
			SELECT
				UNNEST($8::text[]) AS tx_hash,
				UNNEST($9::text[]) AS account_id
		) ta
		WHERE ta.account_id IN (SELECT stellar_address FROM existing_accounts)
    ),

	-- STEP 3: Insert those transactions
    inserted_transactions AS (
        INSERT INTO transactions
          	(hash, to_id, envelope_xdr, result_xdr, meta_xdr, ledger_number, ledger_created_at)
        SELECT
            vt.tx_hash,
            t.to_id,
            t.envelope_xdr,
            t.result_xdr,
            t.meta_xdr,
            t.ledger_number,
            t.ledger_created_at
        FROM valid_transactions vt
        JOIN (
            SELECT
                UNNEST($1::text[]) AS hash,
                UNNEST($2::bigint[]) AS to_id,
                UNNEST($3::text[]) AS envelope_xdr,
                UNNEST($4::text[]) AS result_xdr,
                UNNEST($5::text[]) AS meta_xdr,
                UNNEST($6::bigint[]) AS ledger_number,
                UNNEST($7::timestamptz[]) AS ledger_created_at
        ) t ON t.hash = vt.tx_hash
        ON CONFLICT (hash) DO NOTHING
        RETURNING hash
    ),

	-- STEP 4: Insert transactions_accounts links
	inserted_transactions_accounts AS (
		INSERT INTO transactions_accounts
			(tx_hash, account_id)
		SELECT
			ta.tx_hash, ta.account_id
		FROM (
			SELECT
				UNNEST($8::text[]) AS tx_hash,
				UNNEST($9::text[]) AS account_id
		) ta
		INNER JOIN existing_accounts ea ON ea.stellar_address = ta.account_id
		INNER JOIN inserted_transactions it ON it.hash = ta.tx_hash
		ON CONFLICT DO NOTHING
	)

	-- STEP 5: Return the hashes of successfully inserted transactions
	SELECT hash FROM inserted_transactions;
    `

	start := time.Now()
	var insertedHashes []string
	err := sqlExecuter.SelectContext(ctx, &insertedHashes, insertQuery,
		pq.Array(hashes),
		pq.Array(toIDs),
		pq.Array(envelopeXDRs),
		pq.Array(resultXDRs),
		pq.Array(metaXDRs),
		pq.Array(ledgerNumbers),
		pq.Array(ledgerCreatedAts),
		pq.Array(txHashes),
		pq.Array(stellarAddresses),
	)
	duration := time.Since(start).Seconds()
	for _, dbTableName := range []string{"transactions", "transactions_accounts"} {
		m.MetricsService.ObserveDBQueryDuration("INSERT", dbTableName, duration)
		if err == nil {
			m.MetricsService.IncDBQuery("INSERT", dbTableName)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("batch inserting transactions and transactions_accounts: %w", err)
	}

	return insertedHashes, nil
}
