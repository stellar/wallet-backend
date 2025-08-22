package httphandler

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/go-chi/chi"
	"github.com/google/uuid"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/services"
)

func TestAccountHandlerRegisterAccount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	mockMetricsService := metrics.NewMockMetricsService()

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	accountService, err := services.NewAccountService(models, mockMetricsService)
	require.NoError(t, err)
	handler := &AccountHandler{
		AccountService: accountService,
	}

	// Setup router
	r := chi.NewRouter()
	r.Post("/accounts/{address}", handler.RegisterAccount)

	clearAccounts := func(ctx context.Context) {
		_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE accounts CASCADE")
		require.NoError(t, err)
	}

	t.Run("success_happy_path", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Once()
		mockMetricsService.On("IncActiveAccount").Once()
		defer mockMetricsService.AssertExpectations(t)

		// Prepare request
		address := keypair.MustRandom().Address()
		var req *http.Request
		req, err = http.NewRequest(http.MethodPost, path.Join("/accounts", address), nil)
		require.NoError(t, err)

		// Serve request
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		// Assert 200 response
		assert.Equal(t, http.StatusOK, rr.Code)

		ctx := context.Background()
		var dbAddress sql.NullString
		err = dbConnectionPool.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts")
		require.NoError(t, err)

		// Assert address persisted in DB
		assert.True(t, dbAddress.Valid)
		assert.Equal(t, address, dbAddress.String)

		clearAccounts(ctx)
	})

	t.Run("address_already_exists", func(t *testing.T) {
		mockMetricsService.On("ObserveDBQueryDuration", "INSERT", "accounts", mock.AnythingOfType("float64")).Once()
		mockMetricsService.On("IncDBQuery", "INSERT", "accounts").Once()
		mockMetricsService.On("IncActiveAccount").Once()
		defer mockMetricsService.AssertExpectations(t)

		address := keypair.MustRandom().Address()
		ctx := context.Background()

		// Insert address in DB
		_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
		require.NoError(t, err)

		// Prepare request
		req, err := http.NewRequest(http.MethodPost, path.Join("/accounts", address), nil)
		require.NoError(t, err)

		// Serve request
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		// Assert 200 response
		assert.Equal(t, http.StatusOK, rr.Code)

		var dbAddress sql.NullString
		err = dbConnectionPool.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts")
		require.NoError(t, err)

		// Assert address persisted in DB
		assert.True(t, dbAddress.Valid)
		assert.Equal(t, address, dbAddress.String)

		clearAccounts(ctx)
	})

	t.Run("invalid_address", func(t *testing.T) {
		// Prepare request
		randomString := uuid.NewString()
		req, err := http.NewRequest(http.MethodPost, path.Join("/accounts", randomString), nil)
		require.NoError(t, err)

		// Serve request
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		resp := rr.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.JSONEq(t, `{"error":"Validation error.", "extras": {"address":"Invalid public key provided"}}`, string(respBody))
	})
}

func TestAccountHandlerDeregisterAccount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	mockMetricsService := metrics.NewMockMetricsService()
	mockMetricsService.On("ObserveDBQueryDuration", "DELETE", "accounts", mock.Anything).Return().Times(2)
	mockMetricsService.On("IncDBQuery", "DELETE", "accounts").Return().Times(2)
	mockMetricsService.On("DecActiveAccount").Return().Times(2)
	defer mockMetricsService.AssertExpectations(t)

	models, err := data.NewModels(dbConnectionPool, mockMetricsService)
	require.NoError(t, err)
	accountService, err := services.NewAccountService(models, mockMetricsService)
	require.NoError(t, err)
	handler := &AccountHandler{
		AccountService: accountService,
	}

	// Setup router
	r := chi.NewRouter()
	r.Delete("/accounts/{address}", handler.DeregisterAccount)

	t.Run("successHappyPath", func(t *testing.T) {
		address := keypair.MustRandom().Address()
		ctx := context.Background()

		// Insert address in DB
		_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
		require.NoError(t, err)

		// Prepare request
		var req *http.Request
		req, err = http.NewRequest(http.MethodDelete, path.Join("/accounts", address), nil)
		require.NoError(t, err)

		// Serve request
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		// Assert 200 response
		assert.Equal(t, http.StatusOK, rr.Code)

		// Assert no address no longer in DB
		var dbAddress sql.NullString
		err = dbConnectionPool.GetContext(ctx, &dbAddress, "SELECT stellar_address FROM accounts")
		assert.ErrorIs(t, err, sql.ErrNoRows)
	})

	t.Run("idempotency", func(t *testing.T) {
		address := keypair.MustRandom().Address()
		ctx := context.Background()

		// Make sure DB is empty
		_, err = dbConnectionPool.ExecContext(ctx, "DELETE FROM accounts")
		require.NoError(t, err)

		// Prepare request
		req, err := http.NewRequest(http.MethodDelete, path.Join("/accounts", address), nil)
		require.NoError(t, err)

		// Serve request
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		// Assert 200 response
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("invalid_address", func(t *testing.T) {
		// Prepare request
		randomString := uuid.NewString()
		req, err := http.NewRequest(http.MethodDelete, path.Join("/accounts", randomString), nil)
		require.NoError(t, err)

		// Serve request
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		resp := rr.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.JSONEq(t, `{"error":"Validation error.", "extras": {"address":"Invalid public key provided"}}`, string(respBody))
	})
}

func Test_AccountHandler_SponsorAccountCreation(t *testing.T) {
	ctx := context.Background()

	usdcAsset := entities.Asset{
		Code:   "USDC",
		Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
	}
	masterKP := keypair.MustRandom()
	signerKP1 := keypair.MustRandom()
	signerKP2 := keypair.MustRandom()

	testCases := []struct {
		name               string
		reqBody            string
		setupMocks         func(t *testing.T, asService *services.AccountSponsorshipServiceMock)
		expectedStatusCode int
		expectedRespBody   string
	}{
		{
			name:               "🔴empty_body",
			reqBody:            `{}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedRespBody: `{
				"error": "Validation error.",
				"extras": {
					"address": "This field is required",
					"signers": "This field is required"
				}
			}`,
		},
		{
			name: "🔴invalid_address_and_empty_signers",
			reqBody: `{
				"address": "invalid",
				"signers": []
			}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedRespBody: `{
				"error": "Validation error.",
				"extras": {
					"address": "Invalid public key provided",
					"signers": "Should have at least 1 element(s)"
				}
			}`,
		},
		{
			name: "🔴invalid_assets_and_signers",
			reqBody: `{
				"address": "invalid",
				"signers": [
					{
						"address": "invalid",
						"weight": 0,
						"type": "test"
					}
				],
				"assets": [
					{
						"code": "USDCUSDCUSDCUSDC",
						"issuer": "not-valid"
					}
				]
			}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedRespBody: `{
				"error": "Validation error.",
				"extras": {
					"address": "Invalid public key provided",
					"signers[0].address": "Invalid public key provided",
					"signers[0].type": "Unexpected value \"test\". Expected one of the following values: full, partial",
					"signers[0].weight": "Should be greater than or equal to 1",
					"assets[0].code": "Invalid asset code provided",
					"assets[0].issuer": "Invalid asset issuer provided"
				}
			}`,
		},
		{
			name: "🔴invalid_signers_weight",
			reqBody: `{
				"address": "` + masterKP.Address() + `",
				"signers": [
					{
						"address": "` + signerKP1.Address() + `",
						"weight": 10,
						"type": "full"
					},
					{
						"address": "` + signerKP2.Address() + `",
						"weight": 10,
						"type": "partial"
					}
				]
			}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedRespBody: `{
				"error": "Validation error.",
				"extras": {
					"signers": "all partial signers' weights must be less than the weight of full signers"
				}
			}`,
		},
		{
			name: "🔴account_already_exists",
			reqBody: `{
				"address": "` + masterKP.Address() + `",
				"signers": [
					{
						"address": "` + signerKP1.Address() + `",
						"weight": 10,
						"type": "full"
					}
				]
			}`,
			setupMocks: func(t *testing.T, asService *services.AccountSponsorshipServiceMock) {
				asService.
					On("SponsorAccountCreationTransaction", ctx, services.SponsorAccountCreationOptions{
						Address: masterKP.Address(),
						Signers: []entities.Signer{
							{
								Address: signerKP1.Address(),
								Weight:  10,
								Type:    entities.FullSignerType,
							},
						},
					}).
					Return("", services.ErrAccountAlreadyExists).
					Once()
			},
			expectedStatusCode: http.StatusConflict,
			expectedRespBody:   `{"error": "Account already exists on the Stellar network."}`,
		},
		{
			name: "🔴sponsorship_limit_exceeded",
			reqBody: `{
				"address": "` + masterKP.Address() + `",
				"signers": [
					{
						"address": "` + signerKP1.Address() + `",
						"weight": 10,
						"type": "full"
					}
				]
			}`,
			setupMocks: func(t *testing.T, asService *services.AccountSponsorshipServiceMock) {
				asService.
					On("SponsorAccountCreationTransaction", ctx, services.SponsorAccountCreationOptions{
						Address: masterKP.Address(),
						Signers: []entities.Signer{
							{
								Address: signerKP1.Address(),
								Weight:  10,
								Type:    entities.FullSignerType,
							},
						},
					}).
					Return("", services.ErrSponsorshipLimitExceeded).
					Once()
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedRespBody:   `{"error": "Sponsorship limit exceeded."}`,
		},
		{
			name: "🟢successfully_sponsors_account_creation_without_assets",
			reqBody: `{
				"address": "` + masterKP.Address() + `",
				"signers": [
					{
						"address": "` + signerKP1.Address() + `",
						"weight": 10,
						"type": "full"
					}
				]
			}`,
			setupMocks: func(t *testing.T, asService *services.AccountSponsorshipServiceMock) {
				asService.
					On("SponsorAccountCreationTransaction", ctx, services.SponsorAccountCreationOptions{
						Address: masterKP.Address(),
						Signers: []entities.Signer{
							{
								Address: signerKP1.Address(),
								Weight:  10,
								Type:    entities.FullSignerType,
							},
						},
					}).
					Return("tx-envelope", nil).
					Once()
			},
			expectedStatusCode: http.StatusOK,
			expectedRespBody: `{
				"transaction": "tx-envelope",
				"networkPassphrase": "Test SDF Network ; September 2015"
			}`,
		},
		{
			name: "🟢successfully_sponsors_account_creation_with_assets",
			reqBody: `{
				"address": "` + masterKP.Address() + `",
				"signers": [
					{
						"address": "` + signerKP1.Address() + `",
						"weight": 10,
						"type": "full"
					}
				],
				"assets": [
					{
						"code": "` + usdcAsset.Code + `",
						"issuer": "` + usdcAsset.Issuer + `"
					}
				]
			}`,
			setupMocks: func(t *testing.T, asService *services.AccountSponsorshipServiceMock) {
				asService.
					On("SponsorAccountCreationTransaction", ctx, services.SponsorAccountCreationOptions{
						Address: masterKP.Address(),
						Signers: []entities.Signer{
							{
								Address: signerKP1.Address(),
								Weight:  10,
								Type:    entities.FullSignerType,
							},
						},
						Assets: []entities.Asset{usdcAsset},
					}).
					Return("tx-envelope", nil).
					Once()
			},
			expectedStatusCode: http.StatusOK,
			expectedRespBody: `{
				"transaction": "tx-envelope",
				"networkPassphrase": "Test SDF Network ; September 2015"
			}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Prepare
			asService := services.AccountSponsorshipServiceMock{}
			if tc.setupMocks != nil {
				tc.setupMocks(t, &asService)
				defer asService.AssertExpectations(t)
			}
			handler := &AccountHandler{
				AccountSponsorshipService: &asService,
				NetworkPassphrase:         network.TestNetworkPassphrase,
			}

			// Execute request
			rw := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/tx/create-sponsored-account", strings.NewReader(tc.reqBody))
			http.HandlerFunc(handler.SponsorAccountCreation).ServeHTTP(rw, req)
			resp := rw.Result()
			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			// Assert response
			assert.Equal(t, tc.expectedStatusCode, resp.StatusCode)
			assert.JSONEq(t, tc.expectedRespBody, string(respBody))
		})
	}
}

func TestAccountHandlerCreateFeeBumpTransaction(t *testing.T) {
	asService := services.AccountSponsorshipServiceMock{}
	defer asService.AssertExpectations(t)

	handler := &AccountHandler{
		AccountSponsorshipService: &asService,
	}

	const endpoint = "/tx/create-fee-bump"

	t.Run("invalid_request_body", func(t *testing.T) {
		// Empty body
		reqBody := `{}`
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		http.HandlerFunc(handler.CreateFeeBumpTransaction).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody := `
			{
				"error": "Validation error.",
				"extras": {
					"transaction": "This field is required"
				}
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))

		// Invalid values
		reqBody = `
			{
				"transaction": "invalid transaction envelope"
			}
		`
		rw = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		http.HandlerFunc(handler.CreateFeeBumpTransaction).ServeHTTP(rw, req)

		resp = rw.Result()
		respBody, err = io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody = `
			{
				"error": "Could not parse transaction envelope."
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))

		distributionAccount := keypair.MustRandom()
		accountToSponsor := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: accountToSponsor.Address()},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Amount:      "10",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
			Inner:      tx,
			FeeAccount: distributionAccount.Address(),
			BaseFee:    txnbuild.MinBaseFee,
		})
		require.NoError(t, err)

		feeBumpTxe, err := feeBumpTx.Base64()
		require.NoError(t, err)

		reqBody = fmt.Sprintf(`
			{
				"transaction": %q
			}
		`, feeBumpTxe)
		rw = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		http.HandlerFunc(handler.CreateFeeBumpTransaction).ServeHTTP(rw, req)

		resp = rw.Result()
		respBody, err = io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody = `
			{
				"error": "Cannot accept a fee-bump transaction."
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("account_not_eligible_for_transaction_fee_bump", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: accountToSponsor.Address()},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Amount:      "10.0000000",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		txe, err := tx.Base64()
		require.NoError(t, err)

		reqBody := fmt.Sprintf(`
			{
				"transaction": %q
			}
		`, txe)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		asService.
			On("WrapTransaction", req.Context(), tx).
			Return("", "", services.ErrAccountNotEligibleForBeingSponsored).
			Once()

		http.HandlerFunc(handler.CreateFeeBumpTransaction).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody := `
			{
				"error": "account not eligible for being sponsored"
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("transaction_fee_exceeds_maximum_base_fee_for_sponsoring", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: accountToSponsor.Address()},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Amount:      "10.0000000",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		txe, err := tx.Base64()
		require.NoError(t, err)

		reqBody := fmt.Sprintf(`
			{
				"transaction": %q
			}
		`, txe)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		asService.
			On("WrapTransaction", req.Context(), tx).
			Return("", "", services.ErrFeeExceedsMaximumBaseFee).
			Once()

		http.HandlerFunc(handler.CreateFeeBumpTransaction).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody := `
			{
				"error": "fee exceeds maximum base fee to sponsor"
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("transaction_should_have_at_least_one_signature", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: accountToSponsor.Address()},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Amount:      "10.0000000",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		txe, err := tx.Base64()
		require.NoError(t, err)

		reqBody := fmt.Sprintf(`
			{
				"transaction": %q
			}
		`, txe)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		asService.
			On("WrapTransaction", req.Context(), tx).
			Return("", "", services.ErrNoSignaturesProvided).
			Once()

		http.HandlerFunc(handler.CreateFeeBumpTransaction).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody := `
			{
				"error": "should have at least one signature"
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("blocked_operations", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: accountToSponsor.Address()},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Amount:      "10.0000000",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		txe, err := tx.Base64()
		require.NoError(t, err)

		reqBody := fmt.Sprintf(`
			{
				"transaction": %q
			}
		`, txe)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		asService.
			On("WrapTransaction", req.Context(), tx).
			Return("", "", &services.OperationNotAllowedError{OperationType: xdr.OperationTypeLiquidityPoolDeposit}).
			Once()

		http.HandlerFunc(handler.CreateFeeBumpTransaction).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody := `
			{
				"error": "operation OperationTypeLiquidityPoolDeposit not allowed"
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("successfully_wraps_the_transaction_with_fee_bump", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom()
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: accountToSponsor.Address()},
			IncrementSequenceNum: true,
			Operations: []txnbuild.Operation{
				&txnbuild.Payment{
					Destination: keypair.MustRandom().Address(),
					Amount:      "10.0000000",
					Asset:       txnbuild.NativeAsset{},
				},
			},
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
		})
		require.NoError(t, err)

		txe, err := tx.Base64()
		require.NoError(t, err)

		reqBody := fmt.Sprintf(`
			{
				"transaction": %q
			}
		`, txe)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		asService.
			On("WrapTransaction", req.Context(), tx).
			Return("fee-bump-envelope", network.TestNetworkPassphrase, nil).
			Once()

		http.HandlerFunc(handler.CreateFeeBumpTransaction).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		expectedRespBody := `
			{
				"transaction": "fee-bump-envelope",
				"networkPassphrase": "Test SDF Network ; September 2015"
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})
}
