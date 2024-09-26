package channels

import (
	"context"

	"github.com/alitto/pond"

	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/services"
	"github.com/stellar/wallet-backend/internal/tss/store"
)

type RPCCallerServiceChannelConfigs struct {
	TxManager     services.TransactionManager
	Router        router.Router
	Store         store.Store
	MaxBufferSize int
	MaxWorkers    int
}

type rpcCallerServicePool struct {
	Pool      *pond.WorkerPool
	TxManager services.TransactionManager
	Router    router.Router
	Store     store.Store
}

var ChannelName = "RPCCallerServiceChannel"

func NewRPCCallerServiceChannel(cfg RPCCallerServiceChannelConfigs) *rpcCallerServicePool {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &rpcCallerServicePool{
		Pool:      pool,
		TxManager: cfg.TxManager,
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
	err := p.Store.UpsertTransaction(ctx, payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus})

	if err != nil {
		log.Errorf("RPCCallerChannel: Unable to upsert transaction into transactions table: %e", err)
		return
	}
	rpcSendResp, err := p.TxManager.BuildAndSubmitTransaction(ctx, ChannelName, payload)

	if err != nil {
		log.Errorf("RPCCallerChannel: Unable to sign and submit transaction: %e", err)
		return
	}
	payload.RpcSubmitTxResponse = rpcSendResp
	if rpcSendResp.Status.RPCStatus == entities.TryAgainLaterStatus || rpcSendResp.Status.RPCStatus == entities.ErrorStatus {
		err = p.Router.Route(payload)
		if err != nil {
			log.Errorf("RPCCallerChannel: Unable to route payload: %e", err)
		}
	}
}

func (p *rpcCallerServicePool) SetRouter(router router.Router) {
	p.Router = router
}

func (p *rpcCallerServicePool) Stop() {
	p.Pool.StopAndWait()
}
