package services

import (
	"context"
	"fmt"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	tsserror "github.com/stellar/wallet-backend/internal/tss/errors"
)

type TransactionService interface {
	NetworkPassphrase() string
	SignAndBuildNewFeeBumpTransaction(ctx context.Context, origTxXdr string) (*txnbuild.FeeBumpTransaction, error)
}

type transactionService struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	RPCService                         services.RPCService
	BaseFee                            int64
}

var _ TransactionService = (*transactionService)(nil)

type TransactionServiceOptions struct {
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	RPCService                         services.RPCService
	BaseFee                            int64
}

func (o *TransactionServiceOptions) ValidateOptions() error {
	if o.DistributionAccountSignatureClient == nil {
		return fmt.Errorf("distribution account signature client cannot be nil")
	}

	if o.ChannelAccountSignatureClient == nil {
		return fmt.Errorf("channel account signature client cannot be nil")
	}

	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}

	return nil
}

func NewTransactionService(opts TransactionServiceOptions) (*transactionService, error) {
	if err := opts.ValidateOptions(); err != nil {
		return nil, err
	}
	return &transactionService{
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      opts.ChannelAccountSignatureClient,
		RPCService:                         opts.RPCService,
		BaseFee:                            opts.BaseFee,
	}, nil
}

func (t *transactionService) NetworkPassphrase() string {
	return t.DistributionAccountSignatureClient.NetworkPassphrase()
}

func buildPayments(srcAccount string, operations []txnbuild.Operation) ([]txnbuild.Operation, error) {
	var payments []txnbuild.Operation
	for _, op := range operations {
		origPayment, ok := op.(*txnbuild.Payment)
		if !ok {
			return nil, fmt.Errorf("unable to convert operation to payment op")
		}
		payment := &txnbuild.Payment{
			SourceAccount: srcAccount,
			Amount:        origPayment.Amount,
			Destination:   origPayment.Destination,
			Asset:         origPayment.Asset,
		}
		payments = append(payments, payment)

	}
	return payments, nil
}

func (t *transactionService) SignAndBuildNewFeeBumpTransaction(ctx context.Context, origTxXdr string) (*txnbuild.FeeBumpTransaction, error) {
	genericTx, err := txnbuild.TransactionFromXDR(origTxXdr)
	if err != nil {
		return nil, tsserror.OriginalXDRMalformed
	}
	originalTx, txEmpty := genericTx.Transaction()
	if !txEmpty {
		return nil, tsserror.OriginalXDRMalformed
	}
	channelAccountPublicKey, err := t.ChannelAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting channel account public key: %w", err)
	}
	channelAccountSeq, err := t.RPCService.GetAccountLedgerSequence(channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("getting channel account ledger sequence: %w", err)
	}

	distributionAccountPublicKey, err := t.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting distribution account public key: %w", err)
	}

	operations, err := buildPayments(distributionAccountPublicKey, originalTx.Operations())
	if err != nil {
		return nil, fmt.Errorf("building payment operations: %w", err)
	}
	log.Info(operations)

	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: channelAccountPublicKey,
				Sequence:  channelAccountSeq,
			},
			Operations: operations,
			BaseFee:    int64(t.BaseFee),
			Preconditions: txnbuild.Preconditions{
				TimeBounds: txnbuild.NewTimeout(120),
			},
			IncrementSequenceNum: true,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("building transaction: %w", err)
	}
	tx, err = t.ChannelAccountSignatureClient.SignStellarTransaction(ctx, tx, channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("signing transaction with channel account: %w", err)
	}

	tx, err = t.DistributionAccountSignatureClient.SignStellarTransaction(ctx, tx, distributionAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("signing transaction with distribution account: %w", err)
	}

	feeBumpTx, err := txnbuild.NewFeeBumpTransaction(
		txnbuild.FeeBumpTransactionParams{
			Inner:      tx,
			FeeAccount: distributionAccountPublicKey,
			BaseFee:    int64(t.BaseFee),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("building fee-bump transaction %w", err)
	}

	feeBumpTx, err = t.DistributionAccountSignatureClient.SignStellarFeeBumpTransaction(ctx, feeBumpTx)
	if err != nil {
		return nil, fmt.Errorf("signing the fee bump transaction with distribution account: %w", err)
	}
	return feeBumpTx, nil
}
