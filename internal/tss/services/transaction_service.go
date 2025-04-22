package services

import (
	"context"
	"fmt"

	"github.com/stellar/go/txnbuild"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

const (
	MaxTimeoutInSeconds     = 300
	DefaultTimeoutInSeconds = 10
)

type TransactionService interface {
	NetworkPassphrase() string
	BuildAndSignTransactionWithChannelAccount(ctx context.Context, operations []txnbuild.Operation, timeoutInSecs int64) (*txnbuild.Transaction, error)
	BuildFeeBumpTransaction(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.FeeBumpTransaction, error)
}

type transactionService struct {
	DB                                 db.ConnectionPool
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	ChannelAccountStore                store.ChannelAccountStore
	RPCService                         services.RPCService
	BaseFee                            int64
}

var _ TransactionService = (*transactionService)(nil)

type TransactionServiceOptions struct {
	DB                                 db.ConnectionPool
	DistributionAccountSignatureClient signing.SignatureClient
	ChannelAccountSignatureClient      signing.SignatureClient
	ChannelAccountStore                store.ChannelAccountStore
	RPCService                         services.RPCService
	BaseFee                            int64
}

func (o *TransactionServiceOptions) ValidateOptions() error {
	if o.DB == nil {
		return fmt.Errorf("DB cannot be nil")
	}
	if o.DistributionAccountSignatureClient == nil {
		return fmt.Errorf("distribution account signature client cannot be nil")
	}

	if o.RPCService == nil {
		return fmt.Errorf("rpc client cannot be nil")
	}

	if o.ChannelAccountSignatureClient == nil {
		return fmt.Errorf("channel account signature client cannot be nil")
	}

	if o.ChannelAccountStore == nil {
		return fmt.Errorf("channel account store cannot be nil")
	}

	if o.BaseFee < int64(txnbuild.MinBaseFee) {
		return fmt.Errorf("base fee is lower than the minimum network fee")
	}

	return nil
}

func NewTransactionService(opts TransactionServiceOptions) (*transactionService, error) {
	if err := opts.ValidateOptions(); err != nil {
		return nil, fmt.Errorf("validating transaction service options: %w", err)
	}
	return &transactionService{
		DB:                                 opts.DB,
		DistributionAccountSignatureClient: opts.DistributionAccountSignatureClient,
		ChannelAccountSignatureClient:      opts.ChannelAccountSignatureClient,
		ChannelAccountStore:                opts.ChannelAccountStore,
		RPCService:                         opts.RPCService,
		BaseFee:                            opts.BaseFee,
	}, nil
}

func (t *transactionService) NetworkPassphrase() string {
	return t.DistributionAccountSignatureClient.NetworkPassphrase()
}

func (t *transactionService) BuildAndSignTransactionWithChannelAccount(ctx context.Context, operations []txnbuild.Operation, timeoutInSecs int64) (*txnbuild.Transaction, error) {
	if timeoutInSecs > MaxTimeoutInSeconds {
		return nil, fmt.Errorf("cannot be greater than 300 seconds")
	}
	if timeoutInSecs <= 0 {
		timeoutInSecs = DefaultTimeoutInSeconds
	}
	channelAccountPublicKey, err := t.ChannelAccountSignatureClient.GetAccountPublicKey(ctx, int(timeoutInSecs))
	if err != nil {
		return nil, fmt.Errorf("getting channel account public key: %w", err)
	}
	channelAccountSeq, err := t.RPCService.GetAccountLedgerSequence(channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("getting ledger sequence for channel account public key %q: %w", channelAccountPublicKey, err)
	}
	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: channelAccountPublicKey,
				Sequence:  channelAccountSeq,
			},
			Operations: operations,
			BaseFee:    int64(t.BaseFee),
			Preconditions: txnbuild.Preconditions{
				TimeBounds: txnbuild.NewTimeout(timeoutInSecs),
			},
			IncrementSequenceNum: true,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("building transaction: %w", err)
	}
	txHash, err := tx.HashHex(t.ChannelAccountSignatureClient.NetworkPassphrase())
	if err != nil {
		return nil, fmt.Errorf("unable to hashhex transaction: %w", err)
	}

	err = t.ChannelAccountStore.AssignTxToChannelAccount(ctx, channelAccountPublicKey, txHash)
	if err != nil {
		return nil, fmt.Errorf("assigning channel account to tx: %w", err)
	}

	tx, err = t.ChannelAccountSignatureClient.SignStellarTransaction(ctx, tx, channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("signing transaction with channel account: %w", err)
	}
	return tx, nil
}

func (t *transactionService) BuildFeeBumpTransaction(ctx context.Context, tx *txnbuild.Transaction) (*txnbuild.FeeBumpTransaction, error) {
	distributionAccountPublicKey, err := t.DistributionAccountSignatureClient.GetAccountPublicKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting distribution account public key: %w", err)
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
