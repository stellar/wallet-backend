package httphandler

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"wallet-backend/internal/data"
	"wallet-backend/internal/db/dbtest"

	"wallet-backend/internal/db"

	"github.com/go-chi/chi"
	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeAddress(t *testing.T) {
	dbtest := dbtest.Open(t)
	defer dbtest.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbtest.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	models, err := data.NewModels(dbConnectionPool)
	require.NoError(t, err)
	handler := &PaymentsHandler{
		PaymentModel: models.Payments,
	}

	// Setup router
	r := chi.NewRouter()
	r.Post("/payments/subscribe", handler.SubscribeAddress)

	clearAccounts := func(ctx context.Context) {
		_, err = dbConnectionPool.ExecContext(ctx, "TRUNCATE accounts")
		require.NoError(t, err)
	}

	t.Run("success", func(t *testing.T) {
		// Prepare request
		address := "GANZZQ5WFFDAUVI4RDMRJN2QAEWO7WCLXBVIME6ZXAWO5ZEQA4ZPFWFL"
		payload := fmt.Sprintf(`{ "address": "%s" }`, address)
		req, err := http.NewRequest(http.MethodPost, "/payments/subscribe", strings.NewReader(payload))
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
		address := keypair.MustRandom().Address()
		ctx := context.Background()

		// Insert address in DB
		_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", address)
		require.NoError(t, err)

		// Prepare request
		payload := fmt.Sprintf(`{ "address": "%s" }`, address)
		req, err := http.NewRequest(http.MethodPost, "/payments/subscribe", strings.NewReader(payload))
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
}
