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
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/services/servicesmocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountHandlerSponsorAccountCreation(t *testing.T) {
	asService := servicesmocks.AccountSponsorshipServiceMock{}
	defer asService.AssertExpectations(t)

	assets := []entities.Asset{
		{
			Code:   "USDC",
			Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		},
		{
			Code:   "ARST",
			Issuer: "GB7TAYRUZGE6TVT7NHP5SMIZRNQA6PLM423EYISAOAP3MKYIQMVYP2JO",
		},
	}
	handler := &AccountHandler{
		AccountSponsorshipService: &asService,
		SupportedAssets:           assets,
	}

	const endpoint = "/tx/create-sponsored-account"

	t.Run("invalid_request_body", func(t *testing.T) {
		// Empty body
		reqBody := `{}`
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		http.HandlerFunc(handler.SponsorAccountCreation).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody := `
			{
				"error": "Validation error.",
				"extras": {
					"address": "This field is required",
					"signers": "This field is required"
				}
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))

		// Invalid values
		reqBody = `
			{
				"address": "invalid",
				"signers": []
			}
		`
		rw = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		http.HandlerFunc(handler.SponsorAccountCreation).ServeHTTP(rw, req)

		resp = rw.Result()
		respBody, err = io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody = `
			{
				"error": "Validation error.",
				"extras": {
					"address": "Invalid public key provided",
					"signers": "Should have at least 1 element"
				}
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))

		reqBody = fmt.Sprintf(`
			{
				"address": %q,
				"signers": [
					{
						"address": "invalid",
						"weight": 0,
						"type": "test"
					},
					{
						"address": "invalid",
						"weight": 0,
						"type": "test"
					}
				]
			}
		`, keypair.MustRandom().Address())
		rw = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		http.HandlerFunc(handler.SponsorAccountCreation).ServeHTTP(rw, req)

		resp = rw.Result()
		respBody, err = io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody = `
			{
				"error": "Validation error.",
				"extras": {
					"signers[0].address": "Invalid public key provided",
					"signers[0].type": "Unexpected value \"test\". Expected one of the following values: full, partial",
					"signers[0].weight": "Should be greater than or equal to 1",
					"signers[1].address": "Invalid public key provided",
					"signers[1].type": "Unexpected value \"test\". Expected one of the following values: full, partial",
					"signers[1].weight": "Should be greater than or equal to 1"
				}
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("validate_signers_weight", func(t *testing.T) {
		reqBody := fmt.Sprintf(`
			{
				"address": %q,
				"signers": [
					{
						"address": %q,
						"weight": 10,
						"type": %q
					},
					{
						"address": %q,
						"weight": 10,
						"type": %q
					}
				]
			}
		`, keypair.MustRandom().Address(), keypair.MustRandom().Address(), entities.FullSignerType, keypair.MustRandom().Address(), entities.PartialSignerType)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		http.HandlerFunc(handler.SponsorAccountCreation).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody := `
			{
				"error": "Validation error.",
				"extras": {
					"signers": "all partial signers' weights must be less than the weight of full signers"
				}
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("account_already_exists", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom().Address()
		fullSigner := entities.Signer{
			Address: keypair.MustRandom().Address(),
			Weight:  15,
			Type:    entities.FullSignerType,
		}
		partialSigner := entities.Signer{
			Address: keypair.MustRandom().Address(),
			Weight:  10,
			Type:    entities.PartialSignerType,
		}

		reqBody := fmt.Sprintf(`
			{
				"address": %q,
				"signers": [
					{
						"address": %q,
						"weight": %d,
						"type": %q
					},
					{
						"address": %q,
						"weight": %d,
						"type": %q
					}
				]
			}
		`, accountToSponsor, fullSigner.Address, fullSigner.Weight, fullSigner.Type, partialSigner.Address, partialSigner.Weight, partialSigner.Type)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		asService.
			On("SponsorAccountCreationTransaction", req.Context(), accountToSponsor, []entities.Signer{fullSigner, partialSigner}, assets).
			Return("", "", services.ErrAccountAlreadyExists).
			Once()

		http.HandlerFunc(handler.SponsorAccountCreation).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody := `
			{
				"error": "Account already exists."
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("account_sponsorship_limit_exceeded", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom().Address()
		fullSigner := entities.Signer{
			Address: keypair.MustRandom().Address(),
			Weight:  15,
			Type:    entities.FullSignerType,
		}
		partialSigner := entities.Signer{
			Address: keypair.MustRandom().Address(),
			Weight:  10,
			Type:    entities.PartialSignerType,
		}

		reqBody := fmt.Sprintf(`
			{
				"address": %q,
				"signers": [
					{
						"address": %q,
						"weight": %d,
						"type": %q
					},
					{
						"address": %q,
						"weight": %d,
						"type": %q
					}
				]
			}
		`, accountToSponsor, fullSigner.Address, fullSigner.Weight, fullSigner.Type, partialSigner.Address, partialSigner.Weight, partialSigner.Type)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		asService.
			On("SponsorAccountCreationTransaction", req.Context(), accountToSponsor, []entities.Signer{fullSigner, partialSigner}, assets).
			Return("", "", services.ErrSponsorshipLimitExceeded).
			Once()

		http.HandlerFunc(handler.SponsorAccountCreation).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody := `
			{
				"error": "Sponsorship limit exceeded."
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})

	t.Run("successfully_returns_the_account_sponsorship_transaction_envelope", func(t *testing.T) {
		accountToSponsor := keypair.MustRandom().Address()
		fullSigner := entities.Signer{
			Address: keypair.MustRandom().Address(),
			Weight:  15,
			Type:    entities.FullSignerType,
		}
		partialSigner := entities.Signer{
			Address: keypair.MustRandom().Address(),
			Weight:  10,
			Type:    entities.PartialSignerType,
		}

		reqBody := fmt.Sprintf(`
			{
				"address": %q,
				"signers": [
					{
						"address": %q,
						"weight": %d,
						"type": %q
					},
					{
						"address": %q,
						"weight": %d,
						"type": %q
					}
				]
			}
		`, accountToSponsor, fullSigner.Address, fullSigner.Weight, fullSigner.Type, partialSigner.Address, partialSigner.Weight, partialSigner.Type)
		rw := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, endpoint, strings.NewReader(reqBody))

		asService.
			On("SponsorAccountCreationTransaction", req.Context(), accountToSponsor, []entities.Signer{fullSigner, partialSigner}, assets).
			Return("tx-envelope", network.TestNetworkPassphrase, nil).
			Once()

		http.HandlerFunc(handler.SponsorAccountCreation).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		expectedRespBody := `
			{
				"transaction": "tx-envelope",
				"networkPassphrase": "Test SDF Network ; September 2015"
			}
		`
		assert.JSONEq(t, expectedRespBody, string(respBody))
	})
}

func TestAccountHandlerCreateFeeBumpTransaction(t *testing.T) {
	asService := servicesmocks.AccountSponsorshipServiceMock{}
	defer asService.AssertExpectations(t)

	handler := &AccountHandler{
		AccountSponsorshipService: &asService,
		BlockedOperationsTypes:    []xdr.OperationType{xdr.OperationTypeLiquidityPoolDeposit},
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
			On("WrapTransaction", req.Context(), tx, handler.BlockedOperationsTypes).
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
			On("WrapTransaction", req.Context(), tx, handler.BlockedOperationsTypes).
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
			On("WrapTransaction", req.Context(), tx, handler.BlockedOperationsTypes).
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
			On("WrapTransaction", req.Context(), tx, handler.BlockedOperationsTypes).
			Return("", "", &services.ErrOperationNotAllowed{OperationType: xdr.OperationTypeLiquidityPoolDeposit}).
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
			On("WrapTransaction", req.Context(), tx, handler.BlockedOperationsTypes).
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
