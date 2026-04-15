package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stellar/go-stellar-sdk/keypair"
	godbtest "github.com/stellar/go-stellar-sdk/support/db/dbtest"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	walletdbtest "github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

var (
	complexityTestCtx  context.Context
	complexityTestDB   *godbtest.DB
	complexityTestPool *pgxpool.Pool

	complexityTestAccountAddress = keypair.MustRandom().Address()
	complexityTestTxHash         = "3476b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa4870"
	complexityTestTxToID         = toid.New(1000, 1, 0).ToInt64()
	complexityTestOperationID    = toid.New(1000, 1, 1).ToInt64()
)

type graphQLHTTPResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []graphQLError  `json:"errors"`
}

type graphQLError struct {
	Message    string         `json:"message"`
	Extensions map[string]any `json:"extensions"`
}

func TestMain(m *testing.M) {
	complexityTestCtx = context.Background()

	complexityTestDB = walletdbtest.Open(&testing.T{})
	var err error
	complexityTestPool, err = db.OpenDBConnectionPool(complexityTestCtx, complexityTestDB.DSN)
	if err != nil {
		panic(err)
	}

	setupComplexityTestDB(complexityTestCtx, &testing.T{}, complexityTestPool)

	code := m.Run()

	cleanUpComplexityTestDB(complexityTestCtx, &testing.T{}, complexityTestPool)
	complexityTestPool.Close()
	complexityTestDB.Close()

	os.Exit(code)
}

func TestGraphQLComplexityLimitRejectsLargeQueries(t *testing.T) {
	testCases := []struct {
		name            string
		limit           int
		query           string
		expectedMessage string
	}{
		{
			name:  "account transactions use default page size",
			limit: 100,
			query: `query {
				accountByAddress(address: "` + complexityTestAccountAddress + `") {
					transactions {
						edges {
							node {
								hash
							}
						}
					}
				}
			}`,
			expectedMessage: "operation has complexity 151, which exceeds the limit of 100",
		},
		{
			name:  "account operations use default page size",
			limit: 100,
			query: `query {
				accountByAddress(address: "` + complexityTestAccountAddress + `") {
					operations {
						edges {
							node {
								id
							}
						}
					}
				}
			}`,
			expectedMessage: "operation has complexity 151, which exceeds the limit of 100",
		},
		{
			name:  "account state changes use default page size",
			limit: 100,
			query: `query {
				accountByAddress(address: "` + complexityTestAccountAddress + `") {
					stateChanges {
						edges {
							node {
								ledgerNumber
							}
						}
					}
				}
			}`,
			expectedMessage: "operation has complexity 151, which exceeds the limit of 100",
		},
		{
			name:  "transaction accounts can no longer bypass account pagination complexity",
			limit: 100,
			query: `query {
				transactionByHash(hash: "` + complexityTestTxHash + `") {
					accounts {
						transactions {
							edges {
								node {
									hash
								}
							}
						}
					}
				}
			}`,
			expectedMessage: "operation has complexity 152, which exceeds the limit of 100",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := performGraphQLRequest(t, newGraphQLTestHandler(t, tc.limit), tc.query)

			require.Len(t, resp.Errors, 1)
			assert.Equal(t, tc.expectedMessage, resp.Errors[0].Message)
			assert.Equal(t, "COMPLEXITY_LIMIT_EXCEEDED", resp.Errors[0].Extensions["code"])
			assert.Equal(t, "null", string(resp.Data))
		})
	}
}

func TestGraphQLComplexityAccountingUsesSharedDefaultsAndExplicitArgs(t *testing.T) {
	testCases := []struct {
		name            string
		limit           int
		query           string
		expectedMessage string
	}{
		{
			name:  "root pagination without explicit args uses shared default page limit",
			limit: 149,
			query: `query {
				transactions {
					edges {
						node {
							hash
						}
					}
				}
			}`,
			expectedMessage: "operation has complexity 150, which exceeds the limit of 149",
		},
		{
			name:  "root first argument is used in complexity calculation",
			limit: 5,
			query: `query {
				transactions(first: 2) {
					edges {
						node {
							hash
						}
					}
				}
			}`,
			expectedMessage: "operation has complexity 6, which exceeds the limit of 5",
		},
		{
			name:  "nested last argument is used in complexity calculation",
			limit: 6,
			query: `query {
				transactionByHash(hash: "` + complexityTestTxHash + `") {
					operations(last: 2) {
						edges {
							node {
								id
							}
						}
					}
				}
			}`,
			expectedMessage: "operation has complexity 7, which exceeds the limit of 6",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := performGraphQLRequest(t, newGraphQLTestHandler(t, tc.limit), tc.query)

			require.Len(t, resp.Errors, 1)
			assert.Equal(t, tc.expectedMessage, resp.Errors[0].Message)
			assert.Equal(t, "COMPLEXITY_LIMIT_EXCEEDED", resp.Errors[0].Extensions["code"])
			assert.Equal(t, "null", string(resp.Data))
		})
	}
}

func TestGraphQLComplexityLimitAllowsSmallQueries(t *testing.T) {
	resp := performGraphQLRequest(t, newGraphQLTestHandler(t, 10), `query {
		transactionByHash(hash: "`+complexityTestTxHash+`") {
			hash
		}
	}`)

	require.Empty(t, resp.Errors)
	assert.JSONEq(t, `{
		"transactionByHash": {
			"hash": "`+complexityTestTxHash+`"
		}
	}`, string(resp.Data))
}

func newGraphQLTestHandler(t *testing.T, complexityLimit int) http.Handler {
	t.Helper()

	registry := prometheus.NewRegistry()
	m := metrics.NewMetrics(registry)
	models, err := data.NewModels(complexityTestPool, m.DB)
	require.NoError(t, err)

	return handler(handlerDeps{
		Models:                     models,
		Metrics:                    m,
		TrustlineBalanceModel:      models.TrustlineBalance,
		NativeBalanceModel:         models.NativeBalance,
		SACBalanceModel:            models.SACBalance,
		AccountContractTokensModel: models.AccountContractTokens,
		GraphQLComplexityLimit:     complexityLimit,
		MaxGraphQLWorkerPoolSize:   10,
	})
}

func performGraphQLRequest(t *testing.T, h http.Handler, query string) graphQLHTTPResponse {
	t.Helper()

	body, err := json.Marshal(map[string]string{
		"query": query,
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/graphql/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)

	var resp graphQLHTTPResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	return resp
}

func setupComplexityTestDB(ctx context.Context, t *testing.T, dbConnectionPool *pgxpool.Pool) {
	now := time.Unix(1_700_000_000, 0).UTC()

	transaction := &types.Transaction{
		Hash:            types.HashBytea(complexityTestTxHash),
		ToID:            complexityTestTxToID,
		FeeCharged:      100,
		ResultCode:      "TransactionResultCodeTxSuccess",
		LedgerNumber:    1000,
		LedgerCreatedAt: now,
		IsFeeBump:       false,
	}
	operation := &types.Operation{
		ID:              complexityTestOperationID,
		OperationType:   "PAYMENT",
		OperationXDR:    types.XDRBytea([]byte("opxdr1")),
		ResultCode:      "op_success",
		Successful:      true,
		LedgerNumber:    1000,
		LedgerCreatedAt: now,
	}
	stateChange := &types.StateChange{
		ToID:                complexityTestTxToID,
		StateChangeID:       1,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   types.StateChangeReasonCredit,
		OperationID:         complexityTestOperationID,
		AccountID:           types.AddressBytea(complexityTestAccountAddress),
		LedgerCreatedAt:     now,
		LedgerNumber:        1000,
	}

	dbErr := db.RunInTransaction(ctx, dbConnectionPool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO transactions (hash, to_id, fee_charged, result_code, ledger_number, ledger_created_at, is_fee_bump) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			transaction.Hash, transaction.ToID, transaction.FeeCharged, transaction.ResultCode, transaction.LedgerNumber, transaction.LedgerCreatedAt, transaction.IsFeeBump)
		require.NoError(t, err)

		_, err = tx.Exec(ctx,
			`INSERT INTO transactions_accounts (ledger_created_at, tx_to_id, account_id) VALUES ($1, $2, $3)`,
			transaction.LedgerCreatedAt, transaction.ToID, stateChange.AccountID)
		require.NoError(t, err)

		_, err = tx.Exec(ctx,
			`INSERT INTO operations (id, operation_type, operation_xdr, result_code, successful, ledger_number, ledger_created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			operation.ID, operation.OperationType, operation.OperationXDR, operation.ResultCode, operation.Successful, operation.LedgerNumber, operation.LedgerCreatedAt)
		require.NoError(t, err)

		_, err = tx.Exec(ctx,
			`INSERT INTO operations_accounts (ledger_created_at, operation_id, account_id) VALUES ($1, $2, $3)`,
			operation.LedgerCreatedAt, operation.ID, stateChange.AccountID)
		require.NoError(t, err)

		_, err = tx.Exec(ctx,
			`INSERT INTO state_changes (to_id, state_change_id, state_change_category, state_change_reason, operation_id, account_id, ledger_created_at, ledger_number) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			stateChange.ToID, stateChange.StateChangeID, stateChange.StateChangeCategory, stateChange.StateChangeReason, stateChange.OperationID, stateChange.AccountID, stateChange.LedgerCreatedAt, stateChange.LedgerNumber)
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, dbErr)
}

func cleanUpComplexityTestDB(ctx context.Context, t *testing.T, dbConnectionPool *pgxpool.Pool) {
	_, err := dbConnectionPool.Exec(ctx, `DELETE FROM state_changes`)
	require.NoError(t, err)
	_, err = dbConnectionPool.Exec(ctx, `DELETE FROM operations`)
	require.NoError(t, err)
	_, err = dbConnectionPool.Exec(ctx, `DELETE FROM transactions`)
	require.NoError(t, err)
}
