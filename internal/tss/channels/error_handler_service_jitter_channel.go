package channels

import (
	"context"
	"slices"
	"time"

	"github.com/alitto/pond"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	tss_store "github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
	"golang.org/x/exp/rand"
)

type RPCErrorHandlerServiceJitterChannelConfigs struct {
	Store                tss_store.Store
	TxService            utils.TransactionService
	Router               router.Router
	MaxBufferSize        int
	MaxWorkers           int
	MaxRetries           int
	MinWaitBtwnRetriesMS int
}

type rpcErrorHandlerServiceJitterPool struct {
	Pool                 *pond.WorkerPool
	TxService            utils.TransactionService
	Store                tss_store.Store
	Router               router.Router
	MaxRetries           int
	MinWaitBtwnRetriesMS int
}

func jitter(dur time.Duration) time.Duration {
	halfDur := int64(dur / 2)
	delta := rand.Int63n(halfDur) - halfDur/2
	return dur + time.Duration(delta)
}

func NewErrorHandlerServiceJitterChannel(cfg RPCErrorHandlerServiceJitterChannelConfigs) *rpcErrorHandlerServiceJitterPool {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &rpcErrorHandlerServiceJitterPool{
		Pool:                 pool,
		TxService:            cfg.TxService,
		Store:                cfg.Store,
		MaxRetries:           cfg.MaxRetries,
		MinWaitBtwnRetriesMS: cfg.MinWaitBtwnRetriesMS,
	}
}

func (p *rpcErrorHandlerServiceJitterPool) Send(payload tss.Payload) {
	p.Pool.Submit(func() {
		p.Receive(payload)
	})
}

func (p *rpcErrorHandlerServiceJitterPool) Receive(payload tss.Payload) {
	ctx := context.Background()
	var i int
	for i = 0; i < p.MaxRetries; i++ {
		currentBackoff := p.MinWaitBtwnRetriesMS * (1 << i)
		sleep(jitter(time.Duration(currentBackoff)) * time.Microsecond)
		rpcSendResp, err := SignAndSubmitTransaction(ctx, "ErrorHandlerServiceJitterChannel", payload, p.Store, p.TxService)
		if err != nil {
			log.Errorf(err.Error())
			return
		}
		payload.RpcSubmitTxResponse = rpcSendResp
		if !slices.Contains(tss.JitterErrorCodes, rpcSendResp.Code.TxResultCode) {
			p.Router.Route(payload)
			return
		}
	}
	if i == p.MaxRetries {
		// Retry limit reached, route the payload to the router so it can re-route it to this pool and keep re-trying
		// NOTE: Is this a good idea? Infinite tries per transaction ?
		p.Router.Route(payload)
	}
}

func (p *rpcErrorHandlerServiceJitterPool) SetRouter(router router.Router) {
	p.Router = router
}

func (p *rpcErrorHandlerServiceJitterPool) Stop() {
	p.Pool.StopAndWait()
}
