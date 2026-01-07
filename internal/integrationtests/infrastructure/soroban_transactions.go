// Package infrastructure provides Soroban transaction helpers for integration tests
package infrastructure

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/pkg/sorobanauth"
)

// SimulateTransactionRPC simulates a transaction via RPC to get resource footprint
func SimulateTransactionRPC(client *http.Client, rpcURL, txXDR string) (*entities.RPCSimulateTransactionResult, error) {
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

// SubmitTransactionToRPC submits a transaction XDR to the RPC endpoint
func SubmitTransactionToRPC(client *http.Client, rpcURL, txXDR string) (*entities.RPCSendTransactionResult, error) {
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

// GetTransactionFromRPC polls RPC for transaction status
func GetTransactionFromRPC(client *http.Client, rpcURL, hash string) (*entities.RPCGetTransactionResult, error) {
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

// WaitForTxConfirmationRPC polls RPC directly until transaction is confirmed or times out.
// Returns an error if the transaction fails or is not confirmed within the retry limit.
// This is a lower-level function that doesn't require an RPCService instance.
func WaitForTxConfirmationRPC(
	client *http.Client,
	rpcURL string,
	hash string,
	retries int,
) error {
	var confirmed bool
	timeout := time.Duration(retries) * TransactionPollInterval

	for range retries {
		time.Sleep(TransactionPollInterval)
		txResult, err := GetTransactionFromRPC(client, rpcURL, hash)
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

// SignAuthEntries signs Soroban authorization entries with the provided keypair.
// This is required for CreateContractV2 with constructor and certain contract invocations.
func SignAuthEntries(
	authEntries []xdr.SorobanAuthorizationEntry,
	signer *keypair.Full,
	networkPassphrase string,
	latestLedger int64,
) ([]xdr.SorobanAuthorizationEntry, error) {
	if len(authEntries) == 0 {
		return authEntries, nil
	}

	authSigner := sorobanauth.AuthSigner{
		NetworkPassphrase: networkPassphrase,
	}

	signedAuthEntries := make([]xdr.SorobanAuthorizationEntry, len(authEntries))
	for i, authEntry := range authEntries {
		switch authEntry.Credentials.Type {
		case xdr.SorobanCredentialsTypeSorobanCredentialsAddress:
			signedEntry, err := authSigner.AuthorizeEntry(
				authEntry,
				0, // nonce
				uint32(latestLedger+LedgerValidityBuffer),
				signer,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to sign auth entry %d: %w", i, err)
			}
			signedAuthEntries[i] = signedEntry
		case xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount:
			// Source auth entries don't need explicit signing as they are authorized by transaction signature
			signedAuthEntries[i] = authEntry
		default:
			signedAuthEntries[i] = authEntry
		}
	}

	return signedAuthEntries, nil
}

// ExecuteSorobanOperation executes a Soroban operation with the standard pattern:
// 1. Build transaction for simulation
// 2. Simulate transaction
// 3. Sign auth entries (if required)
// 4. Apply simulation results
// 5. Parse MinResourceFee
// 6. Rebuild transaction with simulation results
// 7. Sign transaction
// 8. Submit to RPC
// 9. Wait for confirmation
// 10. Return transaction hash
//
// This helper consolidates the pattern used across all Soroban contract operations.
func ExecuteSorobanOperation(
	httpClient *http.Client,
	rpcURL string,
	masterAccount *txnbuild.SimpleAccount,
	masterKeyPair *keypair.Full,
	op *txnbuild.InvokeHostFunction,
	requireAuth bool,
	retries int,
) (hash string, err error) {
	// Step 1: Build initial transaction for simulation
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        masterAccount,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		IncrementSequenceNum: false, // Don't increment for simulation
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	if err != nil {
		return "", fmt.Errorf("building transaction: %w", err)
	}

	// Step 2: Simulate transaction to get resource footprint
	txXDR, err := tx.Base64()
	if err != nil {
		return "", fmt.Errorf("encoding transaction for simulation: %w", err)
	}

	simulationResult, err := SimulateTransactionRPC(httpClient, rpcURL, txXDR)
	if err != nil {
		return "", fmt.Errorf("simulating transaction: %w", err)
	}
	if simulationResult.Error != "" {
		return "", fmt.Errorf("simulation failed: %s", simulationResult.Error)
	}

	// Step 3: Sign auth entries if required
	if requireAuth && len(simulationResult.Results) > 0 && len(simulationResult.Results[0].Auth) > 0 {
		signedAuth, authErr := SignAuthEntries(
			simulationResult.Results[0].Auth,
			masterKeyPair,
			NetworkPassphrase,
			simulationResult.LatestLedger,
		)
		if authErr != nil {
			return "", fmt.Errorf("signing auth entries: %w", authErr)
		}
		op.Auth = signedAuth
	}

	// Step 4: Apply simulation results to the operation
	op.Ext = xdr.TransactionExt{
		V:           1,
		SorobanData: &simulationResult.TransactionData,
	}

	// Step 5: Parse MinResourceFee
	minResourceFee, err := strconv.ParseInt(simulationResult.MinResourceFee, 10, 64)
	if err != nil {
		return "", fmt.Errorf("parsing MinResourceFee: %w", err)
	}

	// Step 6: Rebuild transaction with simulation results and increment sequence
	tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        masterAccount,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              minResourceFee + txnbuild.MinBaseFee,
		IncrementSequenceNum: true,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
		},
	})
	if err != nil {
		return "", fmt.Errorf("rebuilding transaction: %w", err)
	}

	// Step 7: Sign with master key
	tx, err = tx.Sign(NetworkPassphrase, masterKeyPair)
	if err != nil {
		return "", fmt.Errorf("signing transaction: %w", err)
	}

	txXDR, err = tx.Base64()
	if err != nil {
		return "", fmt.Errorf("encoding signed transaction: %w", err)
	}

	// Step 8: Submit transaction to RPC
	sendResult, err := SubmitTransactionToRPC(httpClient, rpcURL, txXDR)
	if err != nil {
		return "", fmt.Errorf("submitting transaction: %w", err)
	}
	if sendResult.Status == entities.ErrorStatus {
		return "", fmt.Errorf("transaction failed with status: %s, hash: %s, errorResultXdr: %s",
			sendResult.Status, sendResult.Hash, sendResult.ErrorResultXDR)
	}

	// Step 9: Wait for transaction confirmation
	err = WaitForTxConfirmationRPC(httpClient, rpcURL, sendResult.Hash, retries)
	if err != nil {
		return "", fmt.Errorf("waiting for confirmation: %w", err)
	}

	return sendResult.Hash, nil
}

// ExecuteClassicOperation executes a classic Stellar operation (non-Soroban):
// 1. Build transaction
// 2. Sign transaction
// 3. Submit to RPC
// 4. Wait for confirmation
//
// This helper consolidates the pattern used for classic operations like CreateAccount, ChangeTrust, Payment.
func ExecuteClassicOperation(
	httpClient *http.Client,
	rpcURL string,
	masterAccount *txnbuild.SimpleAccount,
	ops []txnbuild.Operation,
	signers []*keypair.Full,
) (hash string, err error) {
	// Step 1: Build transaction
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        masterAccount,
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
		tx, err = tx.Sign(NetworkPassphrase, signer)
		if err != nil {
			return "", fmt.Errorf("signing transaction: %w", err)
		}
	}

	txXDR, err := tx.Base64()
	if err != nil {
		return "", fmt.Errorf("encoding transaction: %w", err)
	}

	// Step 3: Submit transaction to RPC
	sendResult, err := SubmitTransactionToRPC(httpClient, rpcURL, txXDR)
	if err != nil {
		return "", fmt.Errorf("submitting transaction: %w", err)
	}
	if sendResult.Status == entities.ErrorStatus {
		return "", fmt.Errorf("transaction failed with status: %s", sendResult.Status)
	}

	// Step 4: Wait for transaction confirmation
	err = WaitForTxConfirmationRPC(httpClient, rpcURL, sendResult.Hash, DefaultConfirmationRetries)
	if err != nil {
		return "", fmt.Errorf("waiting for confirmation: %w", err)
	}

	return sendResult.Hash, nil
}
