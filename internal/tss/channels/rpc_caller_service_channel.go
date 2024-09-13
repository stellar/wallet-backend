package channels

import (
	"github.com/alitto/pond"
	"github.com/stellar/wallet-backend/internal/tss"
	tss_services "github.com/stellar/wallet-backend/internal/tss/services"
	tss_store "github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
)

type RPCCallerServiceChannelConfigs struct {
	Store     tss_store.Store
	TxService utils.TransactionService
	// add pool configs here
	MaxBufferSize int
	MaxWorkers    int
}

type rpcCallerServicePool struct {
	pool *pond.WorkerPool
	// some pool config, make a config struct for it
	txService         utils.TransactionService
	errHandlerService tss_services.Service
	store             tss_store.Store
}

func NewRPCCallerServiceChannel(cfg RPCCallerServiceChannelConfigs) tss.Channel {
	// use cfg to build pool
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &rpcCallerServicePool{
		pool:      pool,
		txService: cfg.TxService,
		store:     cfg.Store,
	}
}

func (p *rpcCallerServicePool) Send(payload tss.Payload) {
	p.pool.Submit(func() {
		p.Receive(payload)
	})
}

func (p *rpcCallerServicePool) Receive(payload tss.Payload) {
	err := p.store.UpsertTransaction(payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, tss.NewStatus)
	if err != nil {
		// TODO: log error
		return
	}

	/*
		The reason we return on each error we encounter is so that the transaction status
		stays at NEW, so that it can be picked up for re-processing when this pool is restarted.
	*/
	feeBumpTx, err := p.txService.SignAndBuildNewTransaction(payload.TransactionXDR)
	if err != nil {
		// TODO: log error
		return
	}
	feeBumpTxHash, err := feeBumpTx.HashHex(p.txService.NetworkPassPhrase())
	if err != nil {
		// TODO: log error
		return
	}

	feeBumpTxXDR, err := feeBumpTx.Base64()
	if err != nil {
		// TODO: log error
		return
	}

	err = p.store.UpsertTry(payload.TransactionHash, feeBumpTxHash, feeBumpTxXDR, tss.RPCTXCode{OtherCodes: tss.NewCode})
	if err != nil {
		// TODO: log error
		return
	}
	rpcSendResp, err := p.txService.SendTransaction(feeBumpTxXDR)

	// if the rpc submitTransaction fails, or we cannot unmarshal it's response, we return because we want to retry this transaction
	if rpcSendResp.Code.OtherCodes == tss.RPCFailCode || rpcSendResp.Code.OtherCodes == tss.UnMarshalBinaryCode {
		// TODO: log here
		return
	}

	err = p.store.UpsertTry(payload.TransactionHash, rpcSendResp.TransactionHash, rpcSendResp.TransactionXDR, rpcSendResp.Code)
	if err != nil {
		// TODO: log error
		return
	}

	err = p.store.UpsertTransaction(payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, rpcSendResp.Status)
	if err != nil {
		// TODO: log error
		return
	}

	// route the payload to the Error handler service
	payload.RpcSubmitTxResponse = rpcSendResp
	if rpcSendResp.Status == tss.TryAgainLaterStatus || rpcSendResp.Status == tss.ErrorStatus {
		p.errHandlerService.ProcessPayload(payload)
	}
}

func (p *rpcCallerServicePool) Stop() {
	p.pool.StopAndWait()
}
