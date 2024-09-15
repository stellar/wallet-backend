package channels

import (
	"github.com/alitto/pond"
	"github.com/stellar/go/support/log"
	"github.com/stellar/wallet-backend/internal/tss"
	tss_services "github.com/stellar/wallet-backend/internal/tss/services"
	tss_store "github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
)

type RPCCallerServiceChannelConfigs struct {
	Store             tss_store.Store
	TxService         utils.TransactionService
	ErrHandlerService tss_services.Service
	MaxBufferSize     int
	MaxWorkers        int
}

type rpcCallerServicePool struct {
	pool              WorkerPool
	txService         utils.TransactionService
	errHandlerService tss_services.Service
	store             tss_store.Store
}

func NewRPCCallerServiceChannel(cfg RPCCallerServiceChannelConfigs) tss.Channel {
	pool := pond.New(cfg.MaxBufferSize, cfg.MaxWorkers, pond.Strategy(pond.Balanced()))
	return &rpcCallerServicePool{
		pool:              pool,
		txService:         cfg.TxService,
		errHandlerService: cfg.ErrHandlerService,
		store:             cfg.Store,
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
		log.Errorf("RPCCallerService: Unable to upsert transaction into transactions table: %s", err.Error())
		return
	}
	/*
		The reason we return on each error we encounter is so that the transaction status
		stays at NEW, so that it does not progress any further and
		can be picked up for re-processing when this pool is restarted.
	*/
	feeBumpTx, err := p.txService.SignAndBuildNewTransaction(payload.TransactionXDR)
	if err != nil {
		log.Errorf("RPCCallerService: Unable to sign/build transaction: %s", err.Error())
		return
	}
	feeBumpTxHash, err := feeBumpTx.HashHex(p.txService.NetworkPassPhrase())
	if err != nil {
		log.Errorf("RPCCallerService: Unable to hashhex fee bump transaction: %s", err.Error())
		return
	}

	feeBumpTxXDR, err := feeBumpTx.Base64()
	if err != nil {
		log.Errorf("RPCCallerService: Unable to base64 fee bump transaction: %s", err.Error())
		return
	}
	err = p.store.UpsertTry(payload.TransactionHash, feeBumpTxHash, feeBumpTxXDR, tss.RPCTXCode{OtherCodes: tss.NewCode})
	if err != nil {
		log.Errorf("RPCCallerService: Unable to upsert try in tries table: %s", err.Error())
		return
	}
	rpcSendResp, rpcErr := p.txService.SendTransaction(feeBumpTxXDR)

	err = p.store.UpsertTry(payload.TransactionHash, feeBumpTxHash, feeBumpTxXDR, rpcSendResp.Code)
	if err != nil {
		log.Errorf("RPCCallerService: Unable to upsert try in tries table: %s", err.Error())
		return
	}
	// if the rpc submitTransaction fails, or we cannot unmarshal it's response, we return because we want to retry this transaction
	if rpcErr != nil && rpcSendResp.Code.OtherCodes == tss.RPCFailCode || rpcSendResp.Code.OtherCodes == tss.UnMarshalBinaryCode {
		log.Errorf("RPCCallerService: RPC fail: %s", rpcErr.Error())
		return
	}

	err = p.store.UpsertTransaction(payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, rpcSendResp.Status)
	if err != nil {
		log.Errorf("RPCCallerService:Unable to do the final update of tx in the transactions table: %s", err.Error())
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
