package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/alitto/pond"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/store"
	tssutils "github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stellar/wallet-backend/internal/utils"
)

type WebhookChannelConfigs struct {
	HTTPClient           utils.HTTPClient
	Store                store.Store
	MaxBufferSize        int
	MaxWorkers           int
	MaxRetries           int
	MinWaitBtwnRetriesMS int
	MetricsService       *metrics.MetricsService
}

type webhookPool struct {
	Pool                 *pond.WorkerPool
	Store                store.Store
	HTTPClient           utils.HTTPClient
	MaxRetries           int
	MinWaitBtwnRetriesMS int
	MetricsService       *metrics.MetricsService
}

var WebhookChannelName = "WebhookChannel"

var _ tss.Channel = (*webhookPool)(nil)

func NewWebhookChannel(cfg WebhookChannelConfigs) *webhookPool {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	webhookPool := &webhookPool{
		Pool:                 pool,
		Store:                cfg.Store,
		HTTPClient:           cfg.HTTPClient,
		MaxRetries:           cfg.MaxRetries,
		MinWaitBtwnRetriesMS: cfg.MinWaitBtwnRetriesMS,
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
		log.Errorf("%s: error marshaling payload: %e", WebhookChannelName, err)
		return
	}
	var i int
	sent := false
	ctx := context.Background()
	for i = 0; i < p.MaxRetries; i++ {
		httpResp, err := p.HTTPClient.Post(payload.WebhookURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Errorf("%s: error making POST request to webhook: %e", WebhookChannelName, err)
		} else {
			defer httpResp.Body.Close()
			if httpResp.StatusCode == http.StatusOK {
				sent = true
				err := p.Store.UpsertTransaction(
					ctx, payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.SentStatus})
				if err != nil {
					log.Errorf("%s: error updating transaction status: %e", WebhookChannelName, err)
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
			log.Errorf("%s: error updating transaction status: %e", WebhookChannelName, err)
		}
	}

}

func (p *webhookPool) Stop() {
	p.Pool.StopAndWait()
}
