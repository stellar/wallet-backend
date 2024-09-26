package channels

import (
	"context"
	"slices"
	"time"

	"github.com/alitto/pond"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/services"
	"golang.org/x/exp/rand"
)

type ErrorJitterChannelConfigs struct {
	TxManager            services.TransactionManager
	Router               router.Router
	MaxBufferSize        int
	MaxWorkers           int
	MaxRetries           int
	MinWaitBtwnRetriesMS int
}

type errorJitterPool struct {
	Pool                 *pond.WorkerPool
	TxManager            services.TransactionManager
	Router               router.Router
	MaxRetries           int
	MinWaitBtwnRetriesMS int
}

var ErrorJitterChannelName = "ErrorJitterChannel"

func jitter(dur time.Duration) time.Duration {
	halfDur := int64(dur / 2)
	delta := rand.Int63n(halfDur) - halfDur/2
	return dur + time.Duration(delta)
}

func NewErrorJitterChannel(cfg ErrorJitterChannelConfigs) *errorJitterPool {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &errorJitterPool{
		Pool:                 pool,
		TxManager:            cfg.TxManager,
		Router:               cfg.Router,
		MaxRetries:           cfg.MaxRetries,
		MinWaitBtwnRetriesMS: cfg.MinWaitBtwnRetriesMS,
	}
}

func (p *errorJitterPool) Send(payload tss.Payload) {
	p.Pool.Submit(func() {
		p.Receive(payload)
	})
}

func (p *errorJitterPool) Receive(payload tss.Payload) {
	ctx := context.Background()
	var i int
	for i = 0; i < p.MaxRetries; i++ {
		currentBackoff := p.MinWaitBtwnRetriesMS * (1 << i)
		time.Sleep(jitter(time.Duration(currentBackoff)) * time.Microsecond)
		rpcSendResp, err := p.TxManager.BuildAndSubmitTransaction(ctx, ErrorJitterChannelName, payload)
		if err != nil {
			log.Errorf("%s: Unable to sign and submit transaction: %e", ErrorJitterChannelName, err)
			return
		}
		payload.RpcSubmitTxResponse = rpcSendResp
		if !slices.Contains(tss.JitterErrorCodes, rpcSendResp.Code.TxResultCode) {
			err = p.Router.Route(payload)
			if err != nil {
				log.Errorf("%s: Unable to route payload: %e", ErrorJitterChannelName, err)
				return
			}
			return
		}
	}
	if i == p.MaxRetries {
		// Retry limit reached, route the payload to the router so it can re-route it to this pool and keep re-trying
		// NOTE: Is this a good idea? Infinite tries per transaction ?
		err := p.Router.Route(payload)
		if err != nil {
			log.Errorf("%s: Unable to route payload: %e", ErrorJitterChannelName, err)
		}
	}
}

func (p *errorJitterPool) SetRouter(router router.Router) {
	p.Router = router
}

func (p *errorJitterPool) Stop() {
	p.Pool.StopAndWait()
}
