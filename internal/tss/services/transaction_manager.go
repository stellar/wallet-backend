package services

import (
	"context"
	"fmt"

	"github.com/stellar/go/txnbuild"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/errors"
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

// this function will now take in a new parameter whether to wrap this in a fee bump or not
func (t *transactionManager) BuildAndSubmitTransaction(ctx context.Context, channelName string, payload tss.Payload) (tss.RPCSendTxResponse, error) {
	genericTx, err := txnbuild.TransactionFromXDR(payload.TransactionXDR)
	if err != nil {
		return tss.RPCSendTxResponse{}, errors.OriginalXDRMalformed
	}
	tx, txEmpty := genericTx.Transaction()
	if !txEmpty {
		return tss.RPCSendTxResponse{}, errors.OriginalXDRMalformed
	}
	var tryTxHash string
	var tryTxXDR string
	if payload.FeeBump {
		feeBumpTx, err := t.TxService.BuildFeeBumpTransaction(ctx, tx)
		if err != nil {
			return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to build fee bump transaction: %w", channelName, err)
		}
		tryTxHash, err = feeBumpTx.HashHex(t.TxService.NetworkPassphrase())
		if err != nil {
			return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to hashhex fee bump transaction: %w", channelName, err)
		}
		tryTxXDR, err = feeBumpTx.Base64()
		if err != nil {
			return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to base64 fee bump transaction: %w", channelName, err)
		}

	} else {
		tryTxHash, err = tx.HashHex(t.TxService.NetworkPassphrase())
		if err != nil {
			return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to hashhex transaction: %w", channelName, err)
		}
		tryTxXDR, err = tx.Base64()
		if err != nil {
			return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to base64 transaction: %w", channelName, err)
		}
	}

	err = t.Store.UpsertTry(ctx, payload.TransactionHash, tryTxHash, tryTxXDR, tss.RPCTXStatus{OtherStatus: tss.NewStatus}, tss.RPCTXCode{OtherCodes: tss.NewCode}, "")
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to upsert try in tries table: %w", channelName, err)
	}
	rpcResp, rpcErr := t.RPCService.SendTransaction(tryTxXDR)
	rpcSendResp, parseErr := tss.ParseToRPCSendTxResponse(tryTxHash, rpcResp, rpcErr)

	err = t.Store.UpsertTry(ctx, payload.TransactionHash, tryTxHash, tryTxXDR, rpcSendResp.Status, rpcSendResp.Code, rpcResp.ErrorResultXDR)
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to upsert try in tries table: %s", channelName, err.Error())
	}

	if parseErr != nil {
		return rpcSendResp, fmt.Errorf("%s: RPC fail: %w", channelName, parseErr)
	}

	if parseErr != nil && rpcSendResp.Code.OtherCodes == tss.RPCFailCode || rpcSendResp.Code.OtherCodes == tss.UnmarshalBinaryCode {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: RPC fail: %w", channelName, rpcErr)
	}

	err = t.Store.UpsertTransaction(ctx, payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, rpcSendResp.Status)
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to do the final update of tx in the transactions table: %s", channelName, err.Error())
	}
	return rpcSendResp, nil
}
