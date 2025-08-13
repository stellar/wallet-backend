package httphandler

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/services"
)

func TestAccountHandlerCreateFeeBumpTransaction(t *testing.T) {
	fbService := services.FeeBumpServiceMock{}
	defer fbService.AssertExpectations(t)

	handler := &AccountHandler{
		FeeBumpService: &fbService,
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

		fbService.
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

		fbService.
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

		fbService.
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

		fbService.
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

		fbService.
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
