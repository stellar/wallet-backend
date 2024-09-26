package services

import (
	"context"
	"fmt"

	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/store"
)

type TransactionManager interface {
	BuildAndSubmitTransaction(ctx context.Context, channelName string, payload tss.Payload) (tss.RPCSendTxResponse, error)
}

type TransactionManagerConfigs struct {
	TxService  TransactionService
	RPCService services.RPCService
	Store      store.Store
}

type transactionManager struct {
	TxService  TransactionService
	RPCService services.RPCService
	Store      store.Store
}

func NewTransactionManager(cfg TransactionManagerConfigs) *transactionManager {
	return &transactionManager{
		TxService:  cfg.TxService,
		RPCService: cfg.RPCService,
		Store:      cfg.Store,
	}
}

func (t *transactionManager) BuildAndSubmitTransaction(ctx context.Context, channelName string, payload tss.Payload) (tss.RPCSendTxResponse, error) {
	feeBumpTx, err := t.TxService.SignAndBuildNewFeeBumpTransaction(ctx, payload.TransactionXDR)
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to sign/build transaction: %w", channelName, err)
	}
	feeBumpTxHash, err := feeBumpTx.HashHex(t.TxService.NetworkPassphrase())
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to hashhex fee bump transaction: %w", channelName, err)
	}

	feeBumpTxXDR, err := feeBumpTx.Base64()
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to base64 fee bump transaction: %w", channelName, err)
	}

	err = t.Store.UpsertTry(ctx, payload.TransactionHash, feeBumpTxHash, feeBumpTxXDR, tss.RPCTXCode{OtherCodes: tss.NewCode})
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to upsert try in tries table: %w", channelName, err)
	}
	rpcResp, rpcErr := t.RPCService.SendTransaction(feeBumpTxXDR)
	rpcSendResp, parseErr := tss.ParseToRPCSendTxResponse(feeBumpTxHash, rpcResp, rpcErr)

	err = t.Store.UpsertTry(ctx, payload.TransactionHash, feeBumpTxHash, feeBumpTxXDR, rpcSendResp.Code)
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to upsert try in tries table: %s", channelName, err.Error())
	}

	if parseErr != nil {
		return rpcSendResp, fmt.Errorf("%s: RPC fail: %w", channelName, parseErr)
	}

	if rpcErr != nil && rpcSendResp.Code.OtherCodes == tss.RPCFailCode || rpcSendResp.Code.OtherCodes == tss.UnmarshalBinaryCode {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: RPC fail: %w", channelName, rpcErr)
	}

	err = t.Store.UpsertTransaction(ctx, payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, rpcSendResp.Status)
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to do the final update of tx in the transactions table: %s", channelName, err.Error())
	}
	return rpcSendResp, nil
}
