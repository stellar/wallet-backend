package signing

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/kms/kmsiface"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/txnbuild"

	"github.com/stellar/wallet-backend/internal/signing/awskms"
	"github.com/stellar/wallet-backend/internal/signing/store"
)

var ErrInvalidPublicKeyProvided = errors.New("invalid public key provided")

type kmsSignatureClient struct {
	networkPassphrase            string
	distributionAccountPublicKey string
	keypairStore                 store.KeypairStore
	client                       kmsiface.KMSAPI
	kmsKeyARN                    string
}

var _ SignatureClient = (*kmsSignatureClient)(nil)

func NewKMSSignatureClient(publicKey string, networkPassphrase string, keypairStore store.KeypairStore, client kmsiface.KMSAPI, awsKeyARN string) (*kmsSignatureClient, error) {
	if !strkey.IsValidEd25519PublicKey(publicKey) {
		return nil, ErrInvalidPublicKeyProvided
	}

	if keypairStore == nil {
		return nil, fmt.Errorf("keypair store cannot be nil")
	}

	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}

	awsKeyARN = strings.TrimSpace(awsKeyARN)
	if awsKeyARN == "" {
		return nil, fmt.Errorf("aws key arn cannot be empty")
	}

	return &kmsSignatureClient{
		networkPassphrase:            networkPassphrase,
		distributionAccountPublicKey: publicKey,
		keypairStore:                 keypairStore,
		client:                       client,
		kmsKeyARN:                    awsKeyARN,
	}, nil
}

func (sc *kmsSignatureClient) GetAccountPublicKey(ctx context.Context, _ ...int) (string, error) {
	return sc.distributionAccountPublicKey, nil
}

func (sc *kmsSignatureClient) NetworkPassphrase() string {
	return sc.networkPassphrase
}

func (sc *kmsSignatureClient) SignStellarTransaction(ctx context.Context, tx *txnbuild.Transaction, stellarAccounts ...string) (*txnbuild.Transaction, error) {
	if tx == nil {
		return nil, ErrInvalidTransaction
	}

	if len(stellarAccounts) == 0 {
		return nil, fmt.Errorf("stellar accounts cannot be empty in %T", sc)
	}

	// Ensure that the distribution account is the only account signing the transaction
	for _, stellarAccount := range stellarAccounts {
		if stellarAccount != sc.distributionAccountPublicKey {
			return nil, fmt.Errorf("stellar account %s is not allowed to sign %T", stellarAccount, sc)
		}
	}

	kpFull, err := sc.getKPFull(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting keypair full in %T: %w", sc, err)
	}

	signedTx, err := tx.Sign(sc.NetworkPassphrase(), kpFull)
	if err != nil {
		return nil, fmt.Errorf("signing transaction in %T: %w", sc, err)
	}

	return signedTx, nil
}

func (sc *kmsSignatureClient) SignStellarFeeBumpTransaction(ctx context.Context, feeBumpTx *txnbuild.FeeBumpTransaction) (*txnbuild.FeeBumpTransaction, error) {
	if feeBumpTx == nil {
		return nil, ErrInvalidTransaction
	}

	kpFull, err := sc.getKPFull(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting keypair full in %T: %w", sc, err)
	}

	signedFeeBumpTx, err := feeBumpTx.Sign(sc.NetworkPassphrase(), kpFull)
	if err != nil {
		return nil, fmt.Errorf("signing fee bump transaction: %w", err)
	}

	return signedFeeBumpTx, nil
}

func (sc *kmsSignatureClient) getKPFull(ctx context.Context) (*keypair.Full, error) {
	kp, err := sc.keypairStore.GetByPublicKey(ctx, sc.distributionAccountPublicKey)
	if err != nil {
		return nil, fmt.Errorf("getting keypair for public key %s in %T: %w", sc.distributionAccountPublicKey, sc, err)
	}

	output, err := sc.client.Decrypt(&kms.DecryptInput{
		CiphertextBlob:    kp.EncryptedPrivateKey,
		EncryptionContext: awskms.GetPrivateKeyEncryptionContext(kp.PublicKey),
		KeyId:             &sc.kmsKeyARN,
	})
	if err != nil {
		return nil, fmt.Errorf("decrypting distribution account private key in %T: %w", sc, err)
	}

	kpFull, err := keypair.ParseFull(string(output.Plaintext))
	if err != nil {
		return nil, fmt.Errorf("parsing distribution account private key in %T: %w", sc, err)
	}

	return kpFull, nil
}
