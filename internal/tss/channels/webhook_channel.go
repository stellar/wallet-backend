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

type WebhookHandlerServiceChannelConfigs struct {
	HTTPClient           utils.HTTPClient
	MaxBufferSize        int
	MaxWorkers           int
	MaxRetries           int
	MinWaitBtwnRetriesMS int
}

type webhookHandlerServicePool struct {
	Pool                 *pond.WorkerPool
	HTTPClient           utils.HTTPClient
	MaxRetries           int
	MinWaitBtwnRetriesMS int
}

var _ tss.Channel = (*webhookHandlerServicePool)(nil)

func NewWebhookHandlerServiceChannel(cfg WebhookHandlerServiceChannelConfigs) *webhookHandlerServicePool {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &webhookHandlerServicePool{
		Pool:                 pool,
		HTTPClient:           cfg.HTTPClient,
		MaxRetries:           cfg.MaxRetries,
		MinWaitBtwnRetriesMS: cfg.MinWaitBtwnRetriesMS,
	}

}

func (p *webhookHandlerServicePool) Send(payload tss.Payload) {
	p.Pool.Submit(func() {
		p.Receive(payload)
	})
}

func (p *webhookHandlerServicePool) Receive(payload tss.Payload) {
	resp := tssutils.PayloadTOTSSResponse(payload)
	jsonData, err := json.Marshal(resp)
	if err != nil {
		log.Errorf("WebhookHandlerServiceChannel: error marshaling payload: %s", err.Error())
		return
	}
	var i int
	for i = 0; i < p.MaxRetries; i++ {
		resp, err := p.HTTPClient.Post(payload.WebhookURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Errorf("WebhookHandlerServiceChannel: error making POST request to webhook: %s", err.Error())
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return
		}
		currentBackoff := p.MinWaitBtwnRetriesMS * (1 << i)
		time.Sleep(jitter(time.Duration(currentBackoff)) * time.Microsecond)
	}
}

func (p *webhookHandlerServicePool) Stop() {
	p.Pool.StopAndWait()
}
