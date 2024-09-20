package channels

import (
	"context"

	"github.com/alitto/pond"

	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/services"
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
)

type RPCCallerServiceChannelConfigs struct {
	Store         store.Store
	TxService     utils.TransactionService
	Router        router.Router
	MaxBufferSize int
	MaxWorkers    int
}

type rpcCallerServicePool struct {
	Pool              *pond.WorkerPool
	TxService         utils.TransactionService
	ErrHandlerService services.Service
	Store             store.Store
	Router            router.Router
}

func NewRPCCallerServiceChannel(cfg RPCCallerServiceChannelConfigs) tss.Channel {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &rpcCallerServicePool{
		Pool:      pool,
		TxService: cfg.TxService,
		Store:     cfg.Store,
		Router:    cfg.Router,
	}

}

func (p *rpcCallerServicePool) Send(payload tss.Payload) {
	p.Pool.Submit(func() {
		p.Receive(payload)
	})
}

func (p *rpcCallerServicePool) Receive(payload tss.Payload) {

	ctx := context.Background()
	// Create a new transaction record in the transactions table.
	err := p.Store.UpsertTransaction(ctx, payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.NewStatus)

	if err != nil {
		log.Errorf("Unable to upsert transaction into transactions table: %s", err.Error())
		return
	}

	rpcSendResp, err := BuildAndSubmitTransaction(ctx, "RPCCallerServiceChannel", payload, p.Store, p.TxService)

	if err != nil {
		log.Errorf(": Unable to sign and submit transaction: %s", err.Error())
		return
	}
	payload.RpcSubmitTxResponse = rpcSendResp
	if rpcSendResp.Status == tss.TryAgainLaterStatus || rpcSendResp.Status == tss.ErrorStatus {
		p.Router.Route(payload)
	}
}

func (p *rpcCallerServicePool) SetRouter(router router.Router) {
	p.Router = router
}

func (p *rpcCallerServicePool) Stop() {
	p.Pool.StopAndWait()
}
