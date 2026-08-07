// Package infrastructure provides Soroban transaction helpers for integration tests
package infrastructure

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stellar/wallet-backend/pkg/sorobanauth"
)

// simulateTransactionRPC simulates a transaction via RPC to get resource footprint
func simulateTransactionRPC(client *http.Client, rpcURL, txXDR string) (*entities.RPCSimulateTransactionResult, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "simulateTransaction",
		"params": map[string]string{
			"transaction": txXDR,
		},
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	resp, err := client.Post(rpcURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("posting to RPC: %w", err)
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck
	}()

	var rpcResp struct {
		Result entities.RPCSimulateTransactionResult `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return &rpcResp.Result, nil
}

// submitTransactionToRPC submits a transaction XDR to the RPC endpoint
func submitTransactionToRPC(client *http.Client, rpcURL, txXDR string) (*entities.RPCSendTransactionResult, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "sendTransaction",
		"params": map[string]string{
			"transaction": txXDR,
		},
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	resp, err := client.Post(rpcURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("posting to RPC: %w", err)
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck
	}()

	var rpcResp struct {
		Result entities.RPCSendTransactionResult `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return &rpcResp.Result, nil
}

// getTransactionFromRPC polls RPC for transaction status
func getTransactionFromRPC(client *http.Client, rpcURL, hash string) (*entities.RPCGetTransactionResult, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTransaction",
		"params": map[string]string{
			"hash": hash,
		},
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	resp, err := client.Post(rpcURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("posting to RPC: %w", err)
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck
	}()

	var rpcResp struct {
		Result entities.RPCGetTransactionResult `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return &rpcResp.Result, nil
}

// waitForTransactionConfirmation polls RPC until transaction is confirmed or times out.
// Returns an error if the transaction fails or is not confirmed within the retry limit.
//
//nolint:unparam // ctx and t kept for API consistency and potential future use
func waitForTransactionConfirmation(
	ctx context.Context,
	t *testing.T,
	client *http.Client,
	rpcURL string,
	hash string,
	retries int,
) error {
	var confirmed bool
	timeout := time.Duration(retries) * TransactionPollInterval

	for range retries {
		time.Sleep(TransactionPollInterval)
		txResult, err := getTransactionFromRPC(client, rpcURL, hash)
		if err == nil {
			if txResult.Status == entities.SuccessStatus {
				confirmed = true
				break
			}
			if txResult.Status == entities.FailedStatus {
				return fmt.Errorf("transaction failed with resultXdr: %s", txResult.ResultXDR)
			}
		}
	}

	if !confirmed {
		return fmt.Errorf("transaction not confirmed after %v", timeout)
	}

	return nil
}

// parseAddressToScAddress converts a Stellar address (G... or C...) to xdr.ScAddress.
// G-addresses are account addresses (user wallets).
// C-addresses are contract addresses (smart contracts).
func parseAddressToScAddress(address string) (xdr.ScAddress, error) {
	if len(address) != 56 {
		return xdr.ScAddress{}, fmt.Errorf("invalid address length: expected 56, got %d", len(address))
	}

	if strings.HasPrefix(address, "G") {
		// G-address: Account address (user wallet)
		accountID := xdr.MustAddress(address)
		return xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &accountID,
		}, nil
	} else if strings.HasPrefix(address, "C") {
		// C-address: Contract address (smart contract)
		contractID, err := strkey.Decode(strkey.VersionByteContract, address)
		if err != nil {
			return xdr.ScAddress{}, fmt.Errorf("decoding contract address: %w", err)
		}
		var id xdr.ContractId
		copy(id[:], contractID)
		return xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &id,
		}, nil
	}

	return xdr.ScAddress{}, fmt.Errorf("invalid address format: must start with G or C")
}

// scAddressToString converts an xdr.ScAddress back into its G- or C-address string form. It is
// the inverse of parseAddressToScAddress, used to name the required signer when an auth entry's
// address credentials don't match any available keypair.
func scAddressToString(addr xdr.ScAddress) (string, error) {
	switch addr.Type {
	case xdr.ScAddressTypeScAddressTypeAccount:
		if addr.AccountId == nil {
			return "", fmt.Errorf("account address has a nil AccountId")
		}
		return addr.AccountId.Address(), nil
	case xdr.ScAddressTypeScAddressTypeContract:
		if addr.ContractId == nil {
			return "", fmt.Errorf("contract address has a nil ContractId")
		}
		encoded, err := strkey.Encode(strkey.VersionByteContract, addr.ContractId[:])
		if err != nil {
			return "", fmt.Errorf("encoding contract address: %w", err)
		}
		return encoded, nil
	default:
		return "", fmt.Errorf("unsupported ScAddress type %d", addr.Type)
	}
}

// signAuthEntriesAs signs simulation-returned Soroban authorization entries using whichever
// keypair among signers matches each entry's required address. Source-account-credentialed
// entries need no explicit signature (the transaction signature covers them) and are passed
// through unchanged. Address-credentialed entries are signed preserving the nonce simulation
// assigned to each entry — Soroban nonces are one-shot per (address, nonce), so a hardcoded
// nonce would fail auth on any address's second entry — and an entry no provided keypair can
// sign fails loudly, naming the required address.
func signAuthEntriesAs(
	authEntries []xdr.SorobanAuthorizationEntry,
	signers []*keypair.Full,
	latestLedger int64,
) ([]xdr.SorobanAuthorizationEntry, error) {
	if len(authEntries) == 0 {
		return authEntries, nil
	}

	authSigner := sorobanauth.AuthSigner{NetworkPassphrase: networkPassphrase}
	signed := make([]xdr.SorobanAuthorizationEntry, len(authEntries))
	for i, entry := range authEntries {
		switch entry.Credentials.Type {
		case xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount:
			signed[i] = entry
		case xdr.SorobanCredentialsTypeSorobanCredentialsAddress:
			requiredAddress, err := scAddressToString(entry.Credentials.Address.Address)
			if err != nil {
				return nil, fmt.Errorf("resolving required auth address for entry %d: %w", i, err)
			}

			var matched *keypair.Full
			for _, signer := range signers {
				if signer.Address() == requiredAddress {
					matched = signer
					break
				}
			}
			if matched == nil {
				return nil, fmt.Errorf("no signer available for required auth address %s (entry %d)", requiredAddress, i)
			}

			nonce := int64(entry.Credentials.Address.Nonce)
			signedEntry, err := authSigner.AuthorizeEntry(entry, nonce, uint32(latestLedger+LedgerValidityBuffer), matched)
			if err != nil {
				return nil, fmt.Errorf("signing auth entry %d for %s: %w", i, requiredAddress, err)
			}
			signed[i] = signedEntry
		default:
			signed[i] = entry
		}
	}
	return signed, nil
}

// executeSorobanOperation executes an InvokeHostFunction with source as the transaction source
// account and primary auth signer (any additional required signers are passed via extraSigners):
//
//  1. Resolve source's sequence: the shared master account uses SharedContainers' locally
//     tracked counter (every master submission flows through this executor or
//     executeClassicOperation, both of which advance it); any other actor's CURRENT sequence
//     is fetched from RPC, since SharedContainers doesn't track arbitrary keypairs.
//  2. Build and simulate the transaction.
//  3. Sign simulation-returned auth entries via signAuthEntriesAs: source-account-credentialed
//     entries pass through (the transaction signature covers them); address-credentialed
//     entries are matched to a keypair in [source]+extraSigners and signed preserving the
//     nonce simulation assigned. An entry no keypair can sign fails the operation, naming the
//     required address.
//  4. Re-simulate with the signed auth entries so the resource footprint (and MinResourceFee)
//     reflects their actual signed size, mirroring the double-simulate pattern in
//     Fixtures.prepareSimulateAndSignContractOp.
//  5. Apply the final simulation's SorobanData and MinResourceFee, rebuild incrementing
//     source's sequence, sign with source, submit, and wait for confirmation.
//
//nolint:unparam // extraSigners is nil at every current call site; kept general for operations that need a second signer
func (s *SharedContainers) executeSorobanOperation(
	ctx context.Context,
	t *testing.T,
	op *txnbuild.InvokeHostFunction,
	source *keypair.Full,
	extraSigners []*keypair.Full,
	retries int,
) (string, error) {
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	if err != nil {
		return "", fmt.Errorf("getting RPC connection string: %w", err)
	}

	// Step 1: resolve the source account. The master account drives its locally tracked
	// counter (the IncrementSequenceNum rebuild below advances it), so consecutive master
	// operations skip the RPC read; other actors read their current sequence from RPC.
	var sourceAccount *txnbuild.SimpleAccount
	if source.Address() == s.masterKeyPair.Address() {
		sourceAccount = s.masterAccount
	} else {
		seq, seqErr := getAccountSequenceRPC(s.httpClient, rpcURL, source.Address())
		if seqErr != nil {
			return "", fmt.Errorf("getting source account sequence: %w", seqErr)
		}
		sourceAccount = &txnbuild.SimpleAccount{AccountID: source.Address(), Sequence: seq}
	}

	signers := make([]*keypair.Full, 0, 1+len(extraSigners))
	signers = append(signers, source)
	signers = append(signers, extraSigners...)

	// Step 2: build and simulate.
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        sourceAccount,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: false,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(DefaultTransactionTimeout),
		},
	})
	if err != nil {
		return "", fmt.Errorf("building transaction for simulation: %w", err)
	}
	txXDR, err := tx.Base64()
	if err != nil {
		return "", fmt.Errorf("encoding transaction for simulation: %w", err)
	}
	simulationResult, err := simulateTransactionRPC(s.httpClient, rpcURL, txXDR)
	if err != nil {
		return "", fmt.Errorf("simulating transaction: %w", err)
	}
	if simulationResult.Error != "" {
		return "", fmt.Errorf("simulation failed: %s", simulationResult.Error)
	}

	// Step 3+4: sign auth entries (preserving the simulation nonce) and re-simulate so the
	// resource footprint accounts for the signed entries' size.
	if len(simulationResult.Results) > 0 && len(simulationResult.Results[0].Auth) > 0 {
		signedAuth, authErr := signAuthEntriesAs(simulationResult.Results[0].Auth, signers, simulationResult.LatestLedger)
		if authErr != nil {
			return "", authErr
		}
		op.Auth = signedAuth

		tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        sourceAccount,
			Operations:           []txnbuild.Operation{op},
			BaseFee:              txnbuild.MinBaseFee,
			IncrementSequenceNum: false,
			Preconditions: txnbuild.Preconditions{
				TimeBounds: txnbuild.NewTimeout(DefaultTransactionTimeout),
			},
		})
		if err != nil {
			return "", fmt.Errorf("rebuilding transaction with signed auth: %w", err)
		}
		txXDR, err = tx.Base64()
		if err != nil {
			return "", fmt.Errorf("encoding transaction with signed auth: %w", err)
		}
		simulationResult, err = simulateTransactionRPC(s.httpClient, rpcURL, txXDR)
		if err != nil {
			return "", fmt.Errorf("re-simulating transaction: %w", err)
		}
		if simulationResult.Error != "" {
			return "", fmt.Errorf("re-simulation failed: %s", simulationResult.Error)
		}
	}

	// Step 5: apply resources, rebuild for real submission, sign, submit, wait for confirmation.
	op.Ext = xdr.TransactionExt{
		V:           1,
		SorobanData: &simulationResult.TransactionData,
	}

	minResourceFee, err := strconv.ParseInt(simulationResult.MinResourceFee, 10, 64)
	if err != nil {
		return "", fmt.Errorf("parsing MinResourceFee: %w", err)
	}

	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        sourceAccount,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              minResourceFee + txnbuild.MinBaseFee,
		IncrementSequenceNum: true,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(DefaultTransactionTimeout),
		},
	})
	if err != nil {
		return "", fmt.Errorf("rebuilding transaction: %w", err)
	}

	tx, err = tx.Sign(networkPassphrase, source)
	if err != nil {
		return "", fmt.Errorf("signing transaction: %w", err)
	}
	txXDR, err = tx.Base64()
	if err != nil {
		return "", fmt.Errorf("encoding signed transaction: %w", err)
	}

	sendResult, err := submitTransactionToRPC(s.httpClient, rpcURL, txXDR)
	if err != nil {
		return "", fmt.Errorf("submitting transaction: %w", err)
	}
	if sendResult.Status == entities.ErrorStatus {
		return "", fmt.Errorf("transaction failed with status: %s, hash: %s, errorResultXdr: %s",
			sendResult.Status, sendResult.Hash, sendResult.ErrorResultXDR)
	}

	if err := waitForTransactionConfirmation(ctx, t, s.httpClient, rpcURL, sendResult.Hash, retries); err != nil {
		return "", fmt.Errorf("waiting for confirmation: %w", err)
	}

	return sendResult.Hash, nil
}

// getAccountSequenceRPC fetches an account's current ledger sequence number directly via RPC.
// executeSorobanOperation uses this (rather than a locally tracked counter) for any source other
// than the shared master account, since arbitrary actor keypairs are not tracked by
// SharedContainers.
func getAccountSequenceRPC(client *http.Client, rpcURL, address string) (int64, error) {
	keyXDR, err := utils.GetAccountLedgerKey(address)
	if err != nil {
		return 0, fmt.Errorf("building account ledger key: %w", err)
	}

	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getLedgerEntries",
		"params": map[string][]string{
			"keys": {keyXDR},
		},
	}
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return 0, fmt.Errorf("marshaling request: %w", err)
	}

	resp, err := client.Post(rpcURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return 0, fmt.Errorf("posting to RPC: %w", err)
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck
	}()

	var rpcResp struct {
		Result entities.RPCGetLedgerEntriesResult `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return 0, fmt.Errorf("decoding response: %w", err)
	}
	if rpcResp.Error != nil {
		return 0, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}
	if len(rpcResp.Result.Entries) == 0 {
		return 0, fmt.Errorf("no ledger entry found for account %s", address)
	}

	var data xdr.LedgerEntryData
	if err := xdr.SafeUnmarshalBase64(rpcResp.Result.Entries[0].DataXDR, &data); err != nil {
		return 0, fmt.Errorf("decoding account ledger entry: %w", err)
	}
	account, ok := data.GetAccount()
	if !ok {
		return 0, fmt.Errorf("ledger entry for %s is not an account", address)
	}
	return int64(account.SeqNum), nil
}

// executeClassicOperation executes a classic Stellar operation (non-Soroban):
// 1. Build transaction
// 2. Sign transaction
// 3. Get RPC URL
// 4. Submit to RPC
// 5. Wait for confirmation
//
// This helper consolidates the pattern used for classic operations like CreateAccount, ChangeTrust, Payment.
//
//nolint:unparam // hash kept for API consistency despite not being used by callers
func executeClassicOperation(
	ctx context.Context,
	t *testing.T,
	s *SharedContainers,
	ops []txnbuild.Operation,
	signers []*keypair.Full,
) (hash string, err error) {
	// Step 1: Build transaction
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        s.masterAccount,
		Operations:           ops,
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: true,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	if err != nil {
		return "", fmt.Errorf("building transaction: %w", err)
	}

	// Step 2: Sign with all required signers
	for _, signer := range signers {
		tx, err = tx.Sign(networkPassphrase, signer)
		if err != nil {
			return "", fmt.Errorf("signing transaction: %w", err)
		}
	}

	// Step 3: Get RPC URL
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	if err != nil {
		return "", fmt.Errorf("getting RPC connection string: %w", err)
	}

	txXDR, err := tx.Base64()
	if err != nil {
		return "", fmt.Errorf("encoding transaction: %w", err)
	}

	// Step 4: Submit transaction to RPC
	sendResult, err := submitTransactionToRPC(s.httpClient, rpcURL, txXDR)
	if err != nil {
		return "", fmt.Errorf("submitting transaction: %w", err)
	}
	if sendResult.Status == entities.ErrorStatus {
		return "", fmt.Errorf("transaction failed with status: %s", sendResult.Status)
	}

	// Step 5: Wait for transaction confirmation
	err = waitForTransactionConfirmation(ctx, t, s.httpClient, rpcURL, sendResult.Hash, DefaultConfirmationRetries)
	if err != nil {
		return "", fmt.Errorf("waiting for confirmation: %w", err)
	}

	return sendResult.Hash, nil
}
