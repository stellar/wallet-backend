// Package infrastructure provides Soroban transaction helpers for integration tests.
//
// This file adds two things needed to exercise the Blend v2 pools protocol:
//   - pure ScVal builder functions for the Blend/SEP-40 contract UDTs, and
//   - a Soroban executor that drives a transaction from an arbitrary actor keypair, rather than
//     the shared master account executeSorobanOperation always uses.
package infrastructure

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stellar/wallet-backend/pkg/sorobanauth"
)

// BlendRequest mirrors the Blend v2 pool Request struct. RequestType encodes the pool action:
// 0=Supply, 1=Withdraw, 2=SupplyCollateral, 3=WithdrawCollateral, 4=Borrow, 5=Repay,
// 6=FillUserLiquidationAuction, 7=FillBadDebtAuction, 8=FillInterestAuction,
// 9=DeleteLiquidationAuction.
type BlendRequest struct {
	RequestType uint32
	Address     string
	Amount      *big.Int
}

// BlendReserveConfig mirrors the Blend v2 pool ReserveConfig struct.
type BlendReserveConfig struct {
	CFactor    uint32
	Decimals   uint32
	Enabled    bool
	Index      uint32
	LFactor    uint32
	MaxUtil    uint32
	RBase      uint32
	ROne       uint32
	RThree     uint32
	RTwo       uint32
	Reactivity uint32
	SupplyCap  *big.Int
	Util       uint32
}

// BlendEmissionMetadata mirrors the Blend v2 pool ReserveEmissionMetadata struct.
type BlendEmissionMetadata struct {
	ResIndex uint32
	ResType  uint32
	Share    uint64
}

// scMapEntry is a single symbol-keyed entry destined for an xdr.ScMap. Callers must pass entries
// to scMap in ascending symbol-sort order (Soroban encodes UDT structs as symbol-sorted ScMaps).
type scMapEntry struct {
	key string
	val xdr.ScVal
}

// scAddr converts a G- or C-address into an ScvAddress ScVal.
func scAddr(t *testing.T, addr string) xdr.ScVal {
	t.Helper()
	scAddress, err := parseAddressToScAddress(addr)
	require.NoError(t, err, "parsing address %s", addr)
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddress}
}

// int128MagnitudeBytes converts v into its 16-byte, big-endian, 128-bit two's complement
// representation, or returns an error if v does not fit in 128 bits. Split into a pure function
// (rather than inlined in scI128) so the overflow/negative-encoding logic can be unit tested
// directly, without needing to trigger scI128's t.Fatal path.
func int128MagnitudeBytes(v *big.Int) ([16]byte, error) {
	mag := new(big.Int)
	if v.Sign() < 0 {
		modulus := new(big.Int).Lsh(big.NewInt(1), 128)
		mag.Add(modulus, v)
	} else {
		mag.Set(v)
	}

	b := mag.Bytes()
	if len(b) > 16 {
		return [16]byte{}, fmt.Errorf("i128 value %s overflows 128 bits", v.String())
	}

	var buf [16]byte
	copy(buf[16-len(b):], b)
	return buf, nil
}

// scI128 encodes v as an ScvI128 ScVal, correctly handling magnitudes above 2^63 and negative
// values via 128-bit two's complement. Fails the test if v does not fit in 128 bits.
func scI128(t *testing.T, v *big.Int) xdr.ScVal {
	t.Helper()

	buf, err := int128MagnitudeBytes(v)
	if err != nil {
		t.Fatal(err)
	}

	hi := int64(binary.BigEndian.Uint64(buf[0:8]))
	lo := binary.BigEndian.Uint64(buf[8:16])

	parts := xdr.Int128Parts{Hi: xdr.Int64(hi), Lo: xdr.Uint64(lo)}
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &parts}
}

// scI128FromInt64 encodes a signed 64-bit value as an ScvI128 ScVal, sign-extending into the high
// 64 bits.
func scI128FromInt64(v int64) xdr.ScVal {
	hi := int64(0)
	if v < 0 {
		hi = -1
	}
	parts := xdr.Int128Parts{Hi: xdr.Int64(hi), Lo: xdr.Uint64(uint64(v))} //nolint:gosec // intentional two's complement bit-pattern reinterpretation
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &parts}
}

// scU32 encodes v as an ScvU32 ScVal.
func scU32(v uint32) xdr.ScVal {
	u := xdr.Uint32(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
}

// scU64 encodes v as an ScvU64 ScVal.
func scU64(v uint64) xdr.ScVal {
	u := xdr.Uint64(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
}

// scString encodes s as an ScvString ScVal.
func scString(t *testing.T, s string) xdr.ScVal {
	t.Helper()
	str := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
}

// scSymbol encodes s as an ScvSymbol ScVal.
func scSymbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

// scBytes32 encodes b as an ScvBytes ScVal.
func scBytes32(b [32]byte) xdr.ScVal {
	bs := xdr.ScBytes(b[:])
	return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &bs}
}

// scBool encodes v as an ScvBool ScVal.
func scBool(v bool) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &v}
}

// scVec builds an ScvVec ScVal from the given values, in order.
func scVec(vals ...xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(vals)
	vecPtr := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
}

// mapEntriesSortedError returns a descriptive error if entries are not in strictly ascending
// symbol-sort order (Soroban UDT ScMaps are always symbol-sorted, and duplicate keys are also
// rejected), or nil if they are already sorted. Split out from scMap so the ordering check can be
// unit tested directly, without needing to trigger scMap's t.Fatal path.
func mapEntriesSortedError(entries []scMapEntry) error {
	for i := 1; i < len(entries); i++ {
		if entries[i-1].key >= entries[i].key {
			return fmt.Errorf("scMap entries not symbol-sorted: %q must sort before %q at index %d", entries[i-1].key, entries[i].key, i)
		}
	}
	return nil
}

// scMap builds an ScvMap ScVal (a Soroban UDT struct) from the given entries. Entries MUST be
// passed in ascending symbol-sort order; scMap fails the test otherwise, since Soroban UDT ScMaps
// are always symbol-sorted and an out-of-order map would not match what a real contract call
// produces or expects.
func scMap(t *testing.T, entries ...scMapEntry) xdr.ScVal {
	t.Helper()

	if err := mapEntriesSortedError(entries); err != nil {
		t.Fatal(err)
	}

	m := make(xdr.ScMap, 0, len(entries))
	for _, e := range entries {
		sym := xdr.ScSymbol(e.key)
		m = append(m, xdr.ScMapEntry{
			Key: xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			Val: e.val,
		})
	}
	mPtr := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mPtr}
}

// scRequestVec builds a Vec<Request> ScVal from the given Blend requests. Each Request struct
// encodes as a symbol-sorted map with keys "address", "amount", "request_type" (alphabetical
// order, which also matches the struct's declared field order).
func scRequestVec(t *testing.T, reqs []BlendRequest) xdr.ScVal {
	t.Helper()

	vals := make([]xdr.ScVal, 0, len(reqs))
	for _, r := range reqs {
		vals = append(vals, scMap(t,
			scMapEntry{key: "address", val: scAddr(t, r.Address)},
			scMapEntry{key: "amount", val: scI128(t, r.Amount)},
			scMapEntry{key: "request_type", val: scU32(r.RequestType)},
		))
	}
	return scVec(vals...)
}

// scReserveConfig builds a ReserveConfig ScVal. The struct encodes as a symbol-sorted map with 13
// keys; notably "r_three" sorts before "r_two" (byte 'h' < 'w'), and "r_two" sorts before
// "reactivity" ('_' < 'e' in ASCII).
func scReserveConfig(t *testing.T, cfg BlendReserveConfig) xdr.ScVal {
	t.Helper()

	return scMap(t,
		scMapEntry{key: "c_factor", val: scU32(cfg.CFactor)},
		scMapEntry{key: "decimals", val: scU32(cfg.Decimals)},
		scMapEntry{key: "enabled", val: scBool(cfg.Enabled)},
		scMapEntry{key: "index", val: scU32(cfg.Index)},
		scMapEntry{key: "l_factor", val: scU32(cfg.LFactor)},
		scMapEntry{key: "max_util", val: scU32(cfg.MaxUtil)},
		scMapEntry{key: "r_base", val: scU32(cfg.RBase)},
		scMapEntry{key: "r_one", val: scU32(cfg.ROne)},
		scMapEntry{key: "r_three", val: scU32(cfg.RThree)},
		scMapEntry{key: "r_two", val: scU32(cfg.RTwo)},
		scMapEntry{key: "reactivity", val: scU32(cfg.Reactivity)},
		scMapEntry{key: "supply_cap", val: scI128(t, cfg.SupplyCap)},
		scMapEntry{key: "util", val: scU32(cfg.Util)},
	)
}

// scEmissionMetadataVec builds a Vec<ReserveEmissionMetadata> ScVal. Each entry encodes as a
// symbol-sorted map with keys "res_index", "res_type", "share" (share is u64, not u32).
func scEmissionMetadataVec(t *testing.T, metas []BlendEmissionMetadata) xdr.ScVal {
	t.Helper()

	vals := make([]xdr.ScVal, 0, len(metas))
	for _, m := range metas {
		vals = append(vals, scMap(t,
			scMapEntry{key: "res_index", val: scU32(m.ResIndex)},
			scMapEntry{key: "res_type", val: scU32(m.ResType)},
			scMapEntry{key: "share", val: scU64(m.Share)},
		))
	}
	return scVec(vals...)
}

// scSep40Asset builds the SEP-40 Asset::Stellar(Address) enum variant, which encodes as
// Vec[Symbol("Stellar"), Address].
func scSep40Asset(t *testing.T, addr string) xdr.ScVal {
	t.Helper()
	return scVec(scSymbol("Stellar"), scAddr(t, addr))
}

// scSep40OtherAsset builds the SEP-40 Asset::Other(Symbol) enum variant, which encodes as
// Vec[Symbol("Other"), Symbol].
func scSep40OtherAsset(sym string) xdr.ScVal {
	return scVec(scSymbol("Other"), scSymbol(sym))
}

// scSep40StellarAssetVec builds a Vec of SEP-40 Asset::Stellar(Address) variants, one per address.
func scSep40StellarAssetVec(t *testing.T, addrs []string) xdr.ScVal {
	t.Helper()

	vals := make([]xdr.ScVal, 0, len(addrs))
	for _, a := range addrs {
		vals = append(vals, scSep40Asset(t, a))
	}
	return scVec(vals...)
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
// through unchanged. Unlike signAuthEntries (which always signs with a single fixed keypair and
// hardcodes nonce 0, which is only safe for the master account's first-ever Soroban call), this
// preserves the nonce simulation assigned to each entry and fails loudly, naming the required
// address, if no provided keypair can sign it.
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

// getAccountSequenceRPC fetches an account's current ledger sequence number directly via RPC.
// executeSorobanOperationAs uses this (rather than a locally tracked counter) because, unlike the
// shared master account, arbitrary actor keypairs are not tracked by SharedContainers.
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

// executeSorobanOperationAs executes an InvokeHostFunction with source as the transaction source
// account and primary auth signer (any additional required signers are passed via extraSigners).
//
// It mirrors the 11-step pattern of executeSorobanOperation, adapted for an arbitrary actor:
//  1. Fetch source's CURRENT sequence via RPC (no locally tracked counter — only the shared
//     master account gets one, since SharedContainers doesn't own arbitrary actor keypairs).
//  2. Build and simulate the transaction.
//  3. Sign simulation-returned auth entries: source-account-credentialed entries need no
//     signature; address-credentialed entries are matched to a keypair in [source]+extraSigners
//     and signed preserving the nonce simulation assigned (never hardcoded to 0). If no keypair
//     matches, fail with an error naming the required address.
//  4. Re-simulate with the signed auth entries so the resource footprint (and MinResourceFee)
//     reflects their actual signed size, mirroring the double-simulate pattern in
//     Fixtures.prepareSimulateAndSignContractOp.
//  5. Apply the final simulation's SorobanData and MinResourceFee, rebuild incrementing source's
//     sequence, sign with source, submit, and wait for confirmation.
func (s *SharedContainers) executeSorobanOperationAs(
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

	// Step 1: fetch source's current sequence from RPC.
	seq, err := getAccountSequenceRPC(s.httpClient, rpcURL, source.Address())
	if err != nil {
		return "", fmt.Errorf("getting source account sequence: %w", err)
	}
	sourceAccount := &txnbuild.SimpleAccount{AccountID: source.Address(), Sequence: seq}

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
		signedAuth, err := signAuthEntriesAs(simulationResult.Results[0].Auth, signers, simulationResult.LatestLedger)
		if err != nil {
			return "", err
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

// SyncMasterSequence refreshes the master account's local sequence counter from RPC. Call it
// before resuming master-account operations (executeSorobanOperation, executeClassicOperation) if
// the master account may have submitted transactions through a path that doesn't track
// SharedContainers' local counter — e.g. acting as an extraSigner in executeSorobanOperationAs —
// so the counter doesn't drift from the ledger's view of the account's sequence number.
func (s *SharedContainers) SyncMasterSequence(ctx context.Context, t *testing.T) {
	rpcURL, err := s.RPCContainer.GetConnectionString(ctx)
	require.NoError(t, err, "getting RPC connection string")

	seq, err := getAccountSequenceRPC(s.httpClient, rpcURL, s.masterKeyPair.Address())
	require.NoError(t, err, "fetching master account sequence")

	s.masterAccount.Sequence = seq
}
