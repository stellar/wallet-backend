package services

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"

	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/signing"
	"github.com/stellar/wallet-backend/internal/signing/store"
	"github.com/stellar/wallet-backend/pkg/utils"
)

const (
	MaxTimeoutInSeconds     = 300
	DefaultTimeoutInSeconds = 10
)

var ErrInvalidArguments = errors.New("invalid arguments")

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
		return nil, fmt.Errorf("%w: timeout cannot be greater than %d seconds", ErrInvalidArguments, MaxTimeoutInSeconds)
	}
	if timeoutInSecs <= 0 {
		timeoutInSecs = DefaultTimeoutInSeconds
	}

	channelAccountPublicKey, err := t.ChannelAccountSignatureClient.GetAccountPublicKey(ctx, int(timeoutInSecs))
	if err != nil {
		return nil, fmt.Errorf("getting channel account public key: %w", err)
	}

	for _, op := range operations {
		// Prevent bad actors from using the channel account as a source account directly.
		if op.GetSourceAccount() == channelAccountPublicKey {
			return nil, fmt.Errorf("%w: operation source account cannot be the channel account public key", ErrInvalidArguments)
		}
		// Prevent bad actors from using the channel account as a source account (inherited from the parent transaction).
		if !utils.IsSorobanTxnbuildOp(op) && op.GetSourceAccount() == "" {
			return nil, fmt.Errorf("%w: operation source account cannot be empty", ErrInvalidArguments)
		}
	}

	channelAccountSeq, err := t.RPCService.GetAccountLedgerSequence(channelAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("getting ledger sequence for channel account public key %q: %w", channelAccountPublicKey, err)
	}

	buildTxParams := txnbuild.TransactionParams{
		SourceAccount: &txnbuild.SimpleAccount{
			AccountID: channelAccountPublicKey,
			Sequence:  channelAccountSeq + 1,
		},
		Operations: operations,
		BaseFee:    int64(t.BaseFee),
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(timeoutInSecs),
		},
		IncrementSequenceNum: false, // <--- using this in combination with seqNum + 1 above because otherwise `handleSorobanFlows` will increment the sequence number again, causing tx_bad_seq.
	}
	buildTxParams, err = t.prepareForSorobanTransaction(ctx, channelAccountPublicKey, buildTxParams)
	if err != nil {
		return nil, fmt.Errorf("handling soroban flows: %w", err)
	}

	tx, err := txnbuild.NewTransaction(buildTxParams)
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

// prepareForSorobanTransaction prepares the transaction params for a soroban transaction.
// It simulates the transaction to get the transaction data, and then sets the transaction ext.
func (t *transactionService) prepareForSorobanTransaction(_ context.Context, channelAccountPublicKey string, buildTxParams txnbuild.TransactionParams) (txnbuild.TransactionParams, error) {
	// Ensure this is a soroban transaction.
	isSoroban := slices.ContainsFunc(buildTxParams.Operations, func(op txnbuild.Operation) bool {
		return utils.IsSorobanTxnbuildOp(op)
	})
	if !isSoroban {
		return buildTxParams, nil
	}

	// When soroban is used, only one operation is allowed.
	if len(buildTxParams.Operations) > 1 {
		return txnbuild.TransactionParams{}, fmt.Errorf("%w: when soroban is used only one operation is allowed", ErrInvalidArguments)
	}

	// Simulate the transaction:
	tx, err := txnbuild.NewTransaction(buildTxParams)
	if err != nil {
		return txnbuild.TransactionParams{}, fmt.Errorf("building transaction: %w", err)
	}
	txXDR, err := tx.Base64()
	if err != nil {
		return txnbuild.TransactionParams{}, fmt.Errorf("unable to base64 transaction: %w", err)
	}
	simulationResponse, err := t.RPCService.SimulateTransaction(txXDR, entities.RPCResourceConfig{})
	if err != nil {
		return txnbuild.TransactionParams{}, fmt.Errorf("simulating transaction: %w", err)
	} else if simulationResponse.Error != "" {
		return txnbuild.TransactionParams{}, fmt.Errorf("%w: transaction simulation failed with error=%s", ErrInvalidArguments, simulationResponse.Error)
	}

	// Check if the channel account public key is used as a source account for any SourceAccount auth entry.
	for _, res := range simulationResponse.Results {
		for _, auth := range res.Auth {
			// TODO: manually generate this and check if it really makes sense.
			if auth.Credentials.Type == xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount {
				if authEntrySigner, innerErr := auth.Credentials.Address.Address.String(); innerErr != nil {
					return txnbuild.TransactionParams{}, fmt.Errorf("%w: unable to get auth entry signer: %w", ErrInvalidArguments, innerErr)
				} else if authEntrySigner == channelAccountPublicKey {
					return txnbuild.TransactionParams{}, fmt.Errorf("%w: operation source account cannot be the channel account public key", ErrInvalidArguments)
				}
			}
		}
	}

	transactionExt, err := xdr.NewTransactionExt(1, simulationResponse.TransactionData)
	if err != nil {
		return txnbuild.TransactionParams{}, fmt.Errorf("unable to create transaction ext: %w", err)
	}

	switch sorobanOp := buildTxParams.Operations[0].(type) {
	case *txnbuild.InvokeHostFunction:
		sorobanOp.Ext = transactionExt
	case *txnbuild.ExtendFootprintTtl:
		sorobanOp.Ext = transactionExt
	case *txnbuild.RestoreFootprint:
		sorobanOp.Ext = transactionExt
	default:
		return txnbuild.TransactionParams{}, fmt.Errorf("%w: operation type %T is not a supported soroban operation", ErrInvalidArguments, buildTxParams.Operations[0])
	}

	// reduce the base fee to 50% since it will be bumped with the resources present in sorobanOp.Ext. If not reduced here, one of the fee bump assertions will fail.
	buildTxParams.BaseFee = int64(math.Max(float64(buildTxParams.BaseFee/2), float64(txnbuild.MinBaseFee)))

	return buildTxParams, nil
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
