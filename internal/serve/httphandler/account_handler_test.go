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
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/services/servicesmocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSponsorAccountCreation(t *testing.T) {
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
		Assets:                    assets,
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
					"signers[0].weight": "Should be greater than or equal 1",
					"signers[1].address": "Invalid public key provided",
					"signers[1].type": "Unexpected value \"test\". Expected one of the following values: full, partial",
					"signers[1].weight": "Should be greater than or equal 1"
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
					"signers": "all partial signers weight must be less than the weight of full signers"
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

	t.Run("account_sponsorship_limit_exceed", func(t *testing.T) {
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
			Return("", "", services.ErrSponsorshipLimitExceed).
			Once()

		http.HandlerFunc(handler.SponsorAccountCreation).ServeHTTP(rw, req)

		resp := rw.Result()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		expectedRespBody := `
			{
				"error": "Sponsorship limit exceed."
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
