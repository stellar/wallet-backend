package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/alitto/pond"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/metrics"
	channelAccountStore "github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/store"
	tssutils "github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stellar/wallet-backend/internal/utils"
)

type WebhookChannelConfigs struct {
	Store                store.Store
	ChannelAccountStore  channelAccountStore.ChannelAccountStore
	HTTPClient           utils.HTTPClient
	MaxRetries           int
	MinWaitBtwnRetriesMS int
	NetworkPassphrase    string
	MaxBufferSize        int
	MaxWorkers           int
	MetricsService       metrics.MetricsService
}

type webhookPool struct {
	Pool                 *pond.WorkerPool
	Store                store.Store
	ChannelAccountStore  channelAccountStore.ChannelAccountStore
	HTTPClient           utils.HTTPClient
	MaxRetries           int
	MinWaitBtwnRetriesMS int
	NetworkPassphrase    string
	MetricsService       metrics.MetricsService
}

var WebhookChannelName = "WebhookChannel"

var _ tss.Channel = (*webhookPool)(nil)

func NewWebhookChannel(cfg WebhookChannelConfigs) *webhookPool {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	webhookPool := &webhookPool{
		Pool:                 pool,
		Store:                cfg.Store,
		ChannelAccountStore:  cfg.ChannelAccountStore,
		HTTPClient:           cfg.HTTPClient,
		MaxRetries:           cfg.MaxRetries,
		MinWaitBtwnRetriesMS: cfg.MinWaitBtwnRetriesMS,
		NetworkPassphrase:    cfg.NetworkPassphrase,
		MetricsService:       cfg.MetricsService,
	}
	if cfg.MetricsService != nil {
		cfg.MetricsService.RegisterPoolMetrics(WebhookChannelName, pool)
	}
	return webhookPool
}

func (p *webhookPool) Send(payload tss.Payload) {
	p.Pool.Submit(func() {
		p.Receive(payload)
	})
}

func (p *webhookPool) Receive(payload tss.Payload) {
	resp := tssutils.PayloadTOTSSResponse(payload)
	jsonData, err := json.Marshal(resp)
	if err != nil {
		err = fmt.Errorf("[%s] error marshaling payload: %w", WebhookChannelName, err)
		log.Error(err)
		return
	}
	var sent bool
	ctx := context.Background()
	err = p.UnlockChannelAccount(ctx, payload.TransactionXDR)
	if err != nil {
		err = fmt.Errorf("[%s] error unlocking channel account from transaction: %w", WebhookChannelName, err)
		log.Error(err)
	}
	for i := range p.MaxRetries {
		httpResp, err := p.HTTPClient.Post(payload.WebhookURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			err = fmt.Errorf("[%s] error making POST request to webhook: %w", WebhookChannelName, err)
			log.Error(err)
		} else {
			defer utils.DeferredClose(ctx, httpResp.Body, "closing response body in the Receive function")
			if httpResp.StatusCode == http.StatusOK {
				sent = true
				err := p.Store.UpsertTransaction(
					ctx, payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.SentStatus})
				if err != nil {
					err = fmt.Errorf("[%s] error updating transaction status: %w", WebhookChannelName, err)
					log.Error(err)
				}
				break
			}
			currentBackoff := p.MinWaitBtwnRetriesMS * (1 << i)
			time.Sleep(jitter(time.Duration(currentBackoff)) * time.Millisecond)
		}
	}
	if !sent {
		err := p.Store.UpsertTransaction(
			ctx, payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NotSentStatus})
		if err != nil {
			err = fmt.Errorf("[%s] error updating transaction status: %w", WebhookChannelName, err)
			log.Error(err)
		}
	}
}

func (p *webhookPool) UnlockChannelAccount(ctx context.Context, txXDR string) error {
	genericTx, err := txnbuild.TransactionFromXDR(txXDR)
	if err != nil {
		return fmt.Errorf("bad transaction xdr: %w", err)
	}
	var tx *txnbuild.Transaction
	feeBumpTx, isFeeBumpTx := genericTx.FeeBump()
	if isFeeBumpTx {
		tx = feeBumpTx.InnerTransaction()
	}
	simpleTx, isTransaction := genericTx.Transaction()
	if isTransaction {
		tx = simpleTx
	}
	txHash, err := tx.HashHex(p.NetworkPassphrase)
	if err != nil {
		return fmt.Errorf("unable to hashhex transaction: %w", err)
	}
	err = p.ChannelAccountStore.UnassignTxAndUnlockChannelAccounts(ctx, txHash)
	if err != nil {
		return fmt.Errorf("unable to unlock channel account associated with transaction: %w", err)
	}
	return nil
}

func (p *webhookPool) Stop() {
	p.Pool.StopAndWait()
}
