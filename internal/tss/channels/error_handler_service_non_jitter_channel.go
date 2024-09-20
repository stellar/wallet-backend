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
)

type RPCErrorHandlerServiceNonJitterChannelConfigs struct {
	Store             tss_store.Store
	TxService         utils.TransactionService
	Router            router.Router
	MaxBufferSize     int
	MaxWorkers        int
	MaxRetries        int
	WaitBtwnRetriesMS int
}

type rpcErrorHandlerServiceNonJitterPool struct {
	Pool              *pond.WorkerPool
	TxService         utils.TransactionService
	Store             tss_store.Store
	Router            router.Router
	MaxRetries        int
	WaitBtwnRetriesMS int
}

func NewErrorHandlerServiceNonJitterChannel(cfg RPCErrorHandlerServiceNonJitterChannelConfigs) *rpcErrorHandlerServiceNonJitterPool {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &rpcErrorHandlerServiceNonJitterPool{
		Pool:              pool,
		TxService:         cfg.TxService,
		Store:             cfg.Store,
		MaxRetries:        cfg.MaxRetries,
		WaitBtwnRetriesMS: cfg.WaitBtwnRetriesMS,
	}
}

func (p *rpcErrorHandlerServiceNonJitterPool) Send(payload tss.Payload) {
	p.Pool.Submit(func() {
		p.Receive(payload)
	})
}

func (p *rpcErrorHandlerServiceNonJitterPool) Receive(payload tss.Payload) {
	ctx := context.Background()
	var i int
	for i = 0; i < p.MaxRetries; i++ {
		sleep(time.Duration(p.WaitBtwnRetriesMS) * time.Microsecond)
		rpcSendResp, err := BuildAndSubmitTransaction(ctx, "ErrorHandlerServiceNonJitterChannel", payload, p.Store, p.TxService)
		if err != nil {
			log.Errorf(err.Error())
			return
		}
		payload.RpcSubmitTxResponse = rpcSendResp
		if !slices.Contains(tss.NonJitterErrorCodes, rpcSendResp.Code.TxResultCode) {
			p.Router.Route(payload)
			return
		}
	}
	if i == p.MaxRetries {
		// Retry limit reached, route the payload to the router so it can re-route it to this pool and keep re-trying
		// NOTE: Is this a good idea?
		p.Router.Route(payload)
	}
}

func (p *rpcErrorHandlerServiceNonJitterPool) SetRouter(router router.Router) {
	p.Router = router
}

func (p *rpcErrorHandlerServiceNonJitterPool) Stop() {
	p.Pool.StopAndWait()
}
