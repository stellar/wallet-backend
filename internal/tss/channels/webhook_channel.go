package channels

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/alitto/pond"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/tss"
	tssutils "github.com/stellar/wallet-backend/internal/tss/utils"
	"github.com/stellar/wallet-backend/internal/utils"
)

type WebhookChannelConfigs struct {
	HTTPClient           utils.HTTPClient
	MaxBufferSize        int
	MaxWorkers           int
	MaxRetries           int
	MinWaitBtwnRetriesMS int
}

type webhookPool struct {
	Pool                 *pond.WorkerPool
	HTTPClient           utils.HTTPClient
	MaxRetries           int
	MinWaitBtwnRetriesMS int
}

var WebhookChannelName = "WebhookChannel"

var _ tss.Channel = (*webhookPool)(nil)

func NewWebhookChannel(cfg WebhookChannelConfigs) *webhookPool {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &webhookPool{
		Pool:                 pool,
		HTTPClient:           cfg.HTTPClient,
		MaxRetries:           cfg.MaxRetries,
		MinWaitBtwnRetriesMS: cfg.MinWaitBtwnRetriesMS,
	}

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
	for i = 0; i < p.MaxRetries; i++ {
		resp, err := p.HTTPClient.Post(payload.WebhookURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Errorf("%s: error making POST request to webhook: %e", WebhookChannelName, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return
		}
		currentBackoff := p.MinWaitBtwnRetriesMS * (1 << i)
		time.Sleep(jitter(time.Duration(currentBackoff)) * time.Millisecond)
	}
}

func (p *webhookPool) Stop() {
	p.Pool.StopAndWait()
}
