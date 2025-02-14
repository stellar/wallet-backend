package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	channelAccountStore "github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/store"
	tssutils "github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookHandlerServiceChannel(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	sqlxDB, err := dbConnectionPool.SqlxDB(context.Background())
	require.NoError(t, err)
	metricsService := metrics.NewMetricsService(sqlxDB)
	store, _ := store.NewStore(dbConnectionPool, metricsService)
	channelAccountStore := channelAccountStore.ChannelAccountStoreMock{}
	mockHTTPClient := utils.MockHTTPClient{}
	cfg := WebhookChannelConfigs{
		HTTPClient:           &mockHTTPClient,
		Store:                store,
		ChannelAccountStore:  &channelAccountStore,
		MaxBufferSize:        1,
		MaxWorkers:           1,
		MaxRetries:           3,
		MinWaitBtwnRetriesMS: 5,
		NetworkPassphrase:    "networkpassphrase",
	}
	channel := NewWebhookChannel(cfg)

	payload := tss.Payload{}
	payload.TransactionHash = "hash"
	payload.TransactionXDR = "xdr"
	payload.WebhookURL = "www.stellar.org"
	jsonData, _ := json.Marshal(tssutils.PayloadTOTSSResponse(payload))

	httpResponse1 := &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "OK"}}`)),
	}

	httpResponse2 := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"result": {"status": "OK"}}`)),
	}

	mockHTTPClient.
		On("Post", payload.WebhookURL, "application/json", bytes.NewBuffer(jsonData)).
		Return(httpResponse1, nil).
		Once()

	mockHTTPClient.
		On("Post", payload.WebhookURL, "application/json", bytes.NewBuffer(jsonData)).
		Return(httpResponse2, nil).
		Once()

	channel.Send(payload)
	channel.Stop()

	mockHTTPClient.AssertNumberOfCalls(t, "Post", 2)

	tx, err := store.GetTransaction(context.Background(), payload.TransactionHash)
	assert.Equal(t, string(tss.SentStatus), tx.Status)
	assert.NoError(t, err)
}

func TestUnlockChannelAccount(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()
	store, _ := store.NewStore(dbConnectionPool)
	channelAccountStore := channelAccountStore.ChannelAccountStoreMock{}
	mockHTTPClient := utils.MockHTTPClient{}
	cfg := WebhookChannelConfigs{
		HTTPClient:           &mockHTTPClient,
		Store:                store,
		ChannelAccountStore:  &channelAccountStore,
		MaxBufferSize:        1,
		MaxWorkers:           1,
		MaxRetries:           3,
		MinWaitBtwnRetriesMS: 5,
		NetworkPassphrase:    "networkpassphrase",
	}
	channel := NewWebhookChannel(cfg)
	account := keypair.MustRandom()
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &txnbuild.SimpleAccount{AccountID: account.Address()},
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

	distributionAccount := keypair.MustRandom()

	feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
		Inner:      tx,
		FeeAccount: distributionAccount.Address(),
		BaseFee:    txnbuild.MinBaseFee,
	})
	require.NoError(t, err)

	t.Run("simple_tx", func(t *testing.T) {
		txXDR, err := tx.Base64()
		assert.NoError(t, err)
		txHash, err := tx.HashHex(cfg.NetworkPassphrase)
		assert.NoError(t, err)
		channelAccountStore.
			On("UnassignTxAndUnlockChannelAccount", context.Background(), txHash).
			Return(nil).
			Once()

		err = channel.UnlockChannelAccount(context.Background(), txXDR)
		assert.NoError(t, err)
	})

	t.Run("feebump_tx", func(t *testing.T) {
		txXDR, err := feeBumpTx.Base64()
		assert.NoError(t, err)
		txHash, err := tx.HashHex(cfg.NetworkPassphrase)
		assert.NoError(t, err)
		channelAccountStore.
			On("UnassignTxAndUnlockChannelAccount", context.Background(), txHash).
			Return(nil).
			Once()

		err = channel.UnlockChannelAccount(context.Background(), txXDR)
		assert.NoError(t, err)
	})

	t.Run("unlock_channel_account_from_tx_returns_error", func(t *testing.T) {
		txXDR, err := tx.Base64()
		assert.NoError(t, err)
		txHash, err := tx.HashHex(cfg.NetworkPassphrase)
		assert.NoError(t, err)
		channelAccountStore.
			On("UnassignTxAndUnlockChannelAccount", context.Background(), txHash).
			Return(errors.New("unabe to unlock channel account")).
			Once()

		err = channel.UnlockChannelAccount(context.Background(), txXDR)
		assert.Equal(t, "unable to unlock channel account associated with transaction: unabe to unlock channel account", err.Error())
	})
}
