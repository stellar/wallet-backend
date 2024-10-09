package channels

import (
	"context"

	"github.com/alitto/pond"

	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/services"
	"github.com/stellar/wallet-backend/internal/tss/store"
)

type RPCCallerChannelConfigs struct {
	TxManager     services.TransactionManager
	Router        router.Router
	Store         store.Store
	MaxBufferSize int
	MaxWorkers    int
}

type rpcCallerPool struct {
	Pool      *pond.WorkerPool
	TxManager services.TransactionManager
	Router    router.Router
	Store     store.Store
}

var RPCCallerChannelName = "RPCCallerChannel"

var _ tss.Channel = (*rpcCallerPool)(nil)

func NewRPCCallerChannel(cfg RPCCallerChannelConfigs) *rpcCallerPool {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &rpcCallerPool{
		Pool:      pool,
		TxManager: cfg.TxManager,
		Store:     cfg.Store,
		Router:    cfg.Router,
	}

}

func (p *rpcCallerPool) Send(payload tss.Payload) {
	p.Pool.Submit(func() {
		p.Receive(payload)
	})
}

func (p *rpcCallerPool) Receive(payload tss.Payload) {

	ctx := context.Background()
	// Create a new transaction record in the transactions table.
	err := p.Store.UpsertTransaction(ctx, payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})

	if err != nil {
		log.Errorf("%s: unable to upsert transaction into transactions table: %e", RPCCallerChannelName, err)
		return
	}
	rpcSendResp, err := p.TxManager.BuildAndSubmitTransaction(ctx, RPCCallerChannelName, payload)

	if err != nil {
		log.Errorf("%s: unable to sign and submit transaction: %e", RPCCallerChannelName, err)
		return
	}
	payload.RpcSubmitTxResponse = rpcSendResp
	err = p.Router.Route(payload)
	if err != nil {
		log.Errorf("%s: unable to route payload: %e", RPCCallerChannelName, err)
	}
}

func (p *rpcCallerPool) SetRouter(router router.Router) {
	p.Router = router
}

func (p *rpcCallerPool) Stop() {
	p.Pool.StopAndWait()
}
