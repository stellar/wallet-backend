package channels

import (
	"fmt"

	"github.com/stellar/wallet-backend/internal/tss"
	"github.com/stellar/wallet-backend/internal/tss/store"
	"github.com/stellar/wallet-backend/internal/tss/utils"
	"golang.org/x/net/context"
)

func SignAndSubmitTransaction(ctx context.Context, channelName string, payload tss.Payload, store store.Store, txService utils.TransactionService) (tss.RPCSendTxResponse, error) {
	feeBumpTx, err := txService.SignAndBuildNewFeeBumpTransaction(ctx, payload.TransactionXDR)
	if err != nil {
		fmt.Println("JERE")
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to sign/build transaction: %s", channelName, err.Error())
	}
	feeBumpTxHash, err := feeBumpTx.HashHex(txService.NetworkPassphrase())
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to hashhex fee bump transaction: %s", channelName, err.Error())
	}

	feeBumpTxXDR, err := feeBumpTx.Base64()
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to base64 fee bump transaction: %s", channelName, err.Error())
	}

	err = store.UpsertTry(ctx, payload.TransactionHash, feeBumpTxHash, feeBumpTxXDR, tss.RPCTXCode{OtherCodes: tss.NewCode})
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to upsert try in tries table: %s", channelName, err.Error())
	}
	rpcSendResp, rpcErr := txService.SendTransaction(feeBumpTxXDR)

	err = store.UpsertTry(ctx, payload.TransactionHash, feeBumpTxHash, feeBumpTxXDR, rpcSendResp.Code)
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to upsert try in tries table: %s", channelName, err.Error())
	}
	if rpcErr != nil && rpcSendResp.Code.OtherCodes == tss.RPCFailCode || rpcSendResp.Code.OtherCodes == tss.UnMarshalBinaryCode {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: RPC fail: %s", channelName, rpcErr.Error())
	}

	err = store.UpsertTransaction(ctx, payload.WebhookURL, payload.TransactionHash, payload.TransactionXDR, rpcSendResp.Status)
	if err != nil {
		return tss.RPCSendTxResponse{}, fmt.Errorf("%s: Unable to do the final update of tx in the transactions table: %s", channelName, err.Error())
	}
	return rpcSendResp, nil
}
