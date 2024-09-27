package channels

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/alitto/pond"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/services"
	tss_store "github.com/stellar/wallet-backend/internal/tss/store"
)

type ErrorNonJitterChannelConfigs struct {
	TxManager         services.TransactionManager
	Router            router.Router
	MaxBufferSize     int
	MaxWorkers        int
	MaxRetries        int
	WaitBtwnRetriesMS int
}

type errorNonJitterPool struct {
	Pool              *pond.WorkerPool
	TxManager         services.TransactionManager
	Store             tss_store.Store
	Router            router.Router
	MaxRetries        int
	WaitBtwnRetriesMS int
}

var ErrorNonJitterChannelName = "ErrorNonJitterChannel"

func NewErrorNonJitterChannel(cfg ErrorNonJitterChannelConfigs) *errorNonJitterPool {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &errorNonJitterPool{
		Pool:              pool,
		TxManager:         cfg.TxManager,
		Router:            cfg.Router,
		MaxRetries:        cfg.MaxRetries,
		WaitBtwnRetriesMS: cfg.WaitBtwnRetriesMS,
	}
}

func (p *errorNonJitterPool) Send(payload tss.Payload) {
	p.Pool.Submit(func() {
		p.Receive(payload)
	})
}

func (p *errorNonJitterPool) Receive(payload tss.Payload) {
	ctx := context.Background()
	var i int
	for i = 0; i < p.MaxRetries; i++ {
		fmt.Println(i)
		time.Sleep(time.Duration(p.WaitBtwnRetriesMS) * time.Microsecond)
		rpcSendResp, err := p.TxManager.BuildAndSubmitTransaction(ctx, ErrorNonJitterChannelName, payload)
		if err != nil {
			log.Errorf("%s: Unable to sign and submit transaction: %e", ErrorNonJitterChannelName, err)
			return
		}
		payload.RpcSubmitTxResponse = rpcSendResp
		if !slices.Contains(tss.NonJitterErrorCodes, rpcSendResp.Code.TxResultCode) {
			err := p.Router.Route(payload)
			if err != nil {
				log.Errorf("%s: Unable to route payload: %e", ErrorNonJitterChannelName, err)
				return
			}
			return
		}
	}
	if i == p.MaxRetries {
		// Retry limit reached, route the payload to the router so it can re-route it to this pool and keep re-trying
		// NOTE: Is this a good idea?
		err := p.Router.Route(payload)
		if err != nil {
			log.Errorf("%s: Unable to route payload: %e", ErrorNonJitterChannelName, err)
			return
		}
	}
}

func (p *errorNonJitterPool) SetRouter(router router.Router) {
	p.Router = router
}

func (p *errorNonJitterPool) Stop() {
	p.Pool.StopAndWait()
}
