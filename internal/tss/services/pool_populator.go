package services

import (
	"context"
	"fmt"
	"slices"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/router"
	"github.com/stellar/wallet-backend/internal/tss/store"
)

type PoolPopulator interface {
	PopulatePools(ctx context.Context)
}

type poolPopulator struct {
	Router     router.Router
	Store      store.Store
	RPCService services.RPCService
}

func NewPoolPopulator(router router.Router, store store.Store, rpcService services.RPCService) (*poolPopulator, error) {
	if router == nil {
		return nil, fmt.Errorf("router is nil")
	}
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	if rpcService == nil {
		return nil, fmt.Errorf("rpcservice is nil")
	}
	return &poolPopulator{
		Router:     router,
		Store:      store,
		RPCService: rpcService,
	}, nil
}

func (p *poolPopulator) PopulatePools(ctx context.Context) {
	err := p.routeNewTransactions(ctx)
	if err != nil {
		log.Ctx(ctx).Errorf("error routing new transactions: %v", err)
	}

	err = p.routeErrorTransactions(ctx)
	if err != nil {
		log.Ctx(ctx).Errorf("error routing error transactions: %v", err)
	}

	err = p.routeFinalTransactions(ctx, tss.RPCTXStatus{RPCStatus: entities.FailedStatus})
	if err != nil {
		log.Ctx(ctx).Errorf("error routing failed transactions: %v", err)
	}

	err = p.routeFinalTransactions(ctx, tss.RPCTXStatus{RPCStatus: entities.SuccessStatus})
	if err != nil {
		log.Ctx(ctx).Errorf("error routing successful transactions: %v", err)
	}

	err = p.routeNotSentTransactions(ctx)
	if err != nil {
		log.Ctx(ctx).Errorf("error routing not_sent transactions: %v", err)
	}
}

func (p *poolPopulator) routeNewTransactions(ctx context.Context) error {
	newTxns, err := p.Store.GetTransactionsWithStatus(ctx, tss.RPCTXStatus{OtherStatus: tss.NewStatus})
	if err != nil {
		return fmt.Errorf("unable to get transactions: %w", err)
	}
	for _, txn := range newTxns {
		payload := tss.Payload{
			TransactionHash: txn.Hash,
			TransactionXDR:  txn.XDR,
			WebhookURL:      txn.WebhookURL,
		}
		try, err := p.Store.GetLatestTry(ctx, txn.Hash)
		if err != nil {
			return fmt.Errorf("getting latest try for transaction: %w", err)
		}
		if try == (store.Try{}) || try.Code == int32(tss.RPCFailCode) || try.Code == int32(tss.NewCode) {
			payload.RpcSubmitTxResponse.Status = tss.RPCTXStatus{OtherStatus: tss.NewStatus}
		}
		err = p.Router.Route(payload)
		if err != nil {
			return fmt.Errorf("unable to route payload: %w", err)
		}
	}
	return nil
}

func (p *poolPopulator) routeErrorTransactions(ctx context.Context) error {
	errorTxns, err := p.Store.GetTransactionsWithStatus(ctx, tss.RPCTXStatus{RPCStatus: entities.ErrorStatus})
	if err != nil {
		return fmt.Errorf("unable to get transactions: %w", err)
	}
	for _, txn := range errorTxns {
		payload := tss.Payload{
			TransactionHash: txn.Hash,
			TransactionXDR:  txn.XDR,
			WebhookURL:      txn.WebhookURL,
		}
		try, err := p.Store.GetLatestTry(ctx, txn.Hash)
		if err != nil {
			return fmt.Errorf("gretting latest try for transaction: %w", err)
		}
		if slices.Contains(tss.FinalCodes, xdr.TransactionResultCode(try.Code)) {
			// route to webhook channel
			payload.RpcSubmitTxResponse = tss.RPCSendTxResponse{
				TransactionHash: try.Hash,
				TransactionXDR:  try.XDR,
				Status:          tss.RPCTXStatus{RPCStatus: entities.ErrorStatus},
				Code:            tss.RPCTXCode{TxResultCode: xdr.TransactionResultCode(try.Code)},
				ErrorResultXDR:  try.ResultXDR,
			}
		} else if try.Code == int32(tss.RPCFailCode) || try.Code == int32(tss.NewCode) {
			// route to the error jitter channel
			payload.RpcSubmitTxResponse = tss.RPCSendTxResponse{
				TransactionHash: try.Hash,
				TransactionXDR:  try.XDR,
				Status:          tss.RPCTXStatus{RPCStatus: entities.TryAgainLaterStatus},
			}

		}
		err = p.Router.Route(payload)
		if err != nil {
			return fmt.Errorf("unable to route payload: %w", err)
		}
	}
	return nil
}

func (p *poolPopulator) routeFinalTransactions(ctx context.Context, status tss.RPCTXStatus) error {
	finalTxns, err := p.Store.GetTransactionsWithStatus(ctx, status)
	if err != nil {
		return fmt.Errorf("unable to get transactions: %w", err)
	}
	for _, txn := range finalTxns {
		payload := tss.Payload{
			TransactionHash: txn.Hash,
			TransactionXDR:  txn.XDR,
			WebhookURL:      txn.WebhookURL,
		}
		try, err := p.Store.GetLatestTry(ctx, txn.Hash)
		if err != nil {
			return fmt.Errorf("gretting latest try for transaction: %w", err)
		}
		payload.RpcGetIngestTxResponse = tss.RPCGetIngestTxResponse{
			Status:      status.RPCStatus,
			Code:        tss.RPCTXCode{TxResultCode: xdr.TransactionResultCode(try.Code)},
			EnvelopeXDR: try.XDR,
			ResultXDR:   try.ResultXDR,
		}
		err = p.Router.Route(payload)
		if err != nil {
			return fmt.Errorf("unable to route payload: %w", err)
		}
	}
	return nil
}

func (p *poolPopulator) routeNotSentTransactions(ctx context.Context) error {
	notSentTxns, err := p.Store.GetTransactionsWithStatus(ctx, tss.RPCTXStatus{OtherStatus: tss.NotSentStatus})
	if err != nil {
		return fmt.Errorf("unable to get transactions: %w", err)
	}
	for _, txn := range notSentTxns {
		payload := tss.Payload{
			TransactionHash: txn.Hash,
			TransactionXDR:  txn.XDR,
			WebhookURL:      txn.WebhookURL,
		}
		try, err := p.Store.GetLatestTry(ctx, txn.Hash)
		if err != nil {
			return fmt.Errorf("gretting latest try for transaction: %w", err)
		}
		payload.RpcSubmitTxResponse = tss.RPCSendTxResponse{
			TransactionHash: try.Hash,
			TransactionXDR:  try.XDR,
			Status:          tss.RPCTXStatus{RPCStatus: entities.RPCStatus(try.Status)},
			Code:            tss.RPCTXCode{TxResultCode: xdr.TransactionResultCode(try.Code)},
			ErrorResultXDR:  try.ResultXDR,
		}
		err = p.Router.Route(payload)
		if err != nil {
			return fmt.Errorf("unable to route payload: %w", err)
		}
	}
	return nil
}
