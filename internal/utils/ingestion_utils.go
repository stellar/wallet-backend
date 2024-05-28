package utils

import (
	"strconv"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"
)

func OperationID(ledgerNumber, txNumber, opNumber int32) int64 {
	return toid.New(ledgerNumber, txNumber, opNumber).ToInt64()
}

func OperationResult(tx ingest.LedgerTransaction, opNumber int) *xdr.OperationResultTr {
	results, _ := tx.Result.OperationResults()
	tr := results[opNumber-1].MustTr()
	return &tr
}

func TransactionID(ledgerNumber, txNumber int32) int64 {
	return toid.New(int32(ledgerNumber), int32(txNumber), 0).ToInt64()
}

func TransactionHash(ledgerMeta xdr.LedgerCloseMeta, txNumber int) string {
	return ledgerMeta.TransactionHash(txNumber - 1).HexString()
}

func Memo(memo xdr.Memo, txHash string) string {
	memoType := memo.Type
	switch memoType {
	case xdr.MemoTypeMemoNone:
		return ""
	case xdr.MemoTypeMemoText:
		return memo.MustText()
	case xdr.MemoTypeMemoId:
		return strconv.FormatUint(uint64(memo.MustId()), 10)
	case xdr.MemoTypeMemoHash:
		return memo.MustHash().HexString()
	case xdr.MemoTypeMemoReturn:
		return memo.MustRetHash().HexString()
	default:
		// TODO: track in Sentry
		// sentry.CaptureException(fmt.Errorf("unknown memo type %q for transaction %s", memoType.String(), txHash))
		return ""
	}
}

func SourceAccount(op xdr.Operation, tx ingest.LedgerTransaction) string {
	account := op.SourceAccount
	if account != nil {
		return account.ToAccountId().Address()
	}

	return tx.Envelope.SourceAccount().ToAccountId().Address()
}

func AssetCode(asset xdr.Asset) string {
	if asset.Type == xdr.AssetTypeAssetTypeNative {
		return "XLM"
	}
	return SanitizeUTF8(asset.GetCode())
}
