package channels

import (
	"github.com/alitto/pond"
	"github.com/stellar/wallet-backend/internal/tss"
	tss_services "github.com/stellar/wallet-backend/internal/tss/services"
	tss_store "github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
)

type rpcCallerServicePool struct {
	pool *pond.WorkerPool
	// some pool config, make a config struct for it
	txService         utils.TransactionService
	errHandlerService tss_services.Service
	store             tss_store.Store
}

func NewRPCCallerServiceChannel(store tss_store.Store, txService utils.TransactionService) (tss.Channel, error) {
	pool := pond.New(10, 0, pond.MinWorkers(10))
	return &rpcCallerServicePool{
		pool:      pool,
		txService: txService,
		store:     store,
	}, nil
}

func (p *rpcCallerServicePool) Send(payload tss.Payload) {
	p.pool.Submit(func() {
		p.Receive(payload)
	})
}

func (p *rpcCallerServicePool) Receive(payload tss.Payload) {
	// maybe: why is sqlsqlExec db.SQLExecuter being passed GetAllByPublicKey in channel_accounts_model.go ?
	err := p.store.UpsertTransaction(payload.ClientID, payload.TransactionHash, payload.TransactionXDR, tss.NewStatus)
	if err != nil {
		// TODO: log error
		return
	}

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

	err = p.store.UpsertTry(payload.TransactionHash, feeBumpTxHash, feeBumpTxXDR, tss.NewCode)
	if err != nil {
		// TODO: log error
		return
	}
	rpcSendResp, err := p.trySendTransaction(feeBumpTxXDR)
	if err != nil {
		// reset the status of the transaction back to NEW so it can be re-processed again
		err = p.store.UpsertTransaction(payload.ClientID, payload.TransactionHash, payload.TransactionXDR, tss.NewStatus)
		if err != nil {
			// TODO: log error
		}
		// TODO: log error
		return
	}

	err = p.processRPCSendTxResponse(payload, rpcSendResp)
	if err != nil {
		// reset the status of the transaction back to NEW so it can be re-processed again
		err = p.store.UpsertTransaction(payload.ClientID, payload.TransactionHash, payload.TransactionXDR, tss.NewStatus)
		if err != nil {
			// TODO: log error
		}
		// TODO: log error
	}
}

func (p *rpcCallerServicePool) trySendTransaction(feeBumpTxXDR string) (tss.RPCSendTxResponse, error) {
	rpcSendResp, err := p.txService.SendTransaction(feeBumpTxXDR)
	if err != nil {
		return tss.RPCSendTxResponse{}, err
	}
	return rpcSendResp, nil
}

func (p *rpcCallerServicePool) processRPCSendTxResponse(payload tss.Payload, resp tss.RPCSendTxResponse) error {
	err := p.store.UpsertTry(payload.TransactionHash, resp.TransactionHash, resp.TransactionXDR, resp.Code)
	if err != nil {
		return err
	}
	err = p.store.UpsertTransaction(payload.TransactionHash, resp.TransactionHash, resp.TransactionXDR, resp.Status)
	if err != nil {
		return err
	}
	payload.RpcSubmitTxResponse = resp
	if resp.Status == tss.TryAgainLaterStatus || resp.Status == tss.ErrorStatus {
		p.errHandlerService.ProcessPayload(payload)
	}
	return nil
}

func (p *rpcCallerServicePool) Stop() {
	p.pool.StopAndWait()
}
