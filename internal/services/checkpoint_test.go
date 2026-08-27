package services

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"iter"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
)

// Test helpers

// makeContractCodeEntry builds a ContractCode ledger entry. The live checkpoint reader
// wraps it in an ingest.Change; the hot-archive iterator yields it bare.
func makeContractCodeEntry(hash xdr.Hash, code []byte) xdr.LedgerEntry {
	return xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractCode,
			ContractCode: &xdr.ContractCodeEntry{
				Hash: hash,
				Code: code,
			},
		},
	}
}

func makeContractCodeChange(hash xdr.Hash, code []byte) ingest.Change {
	entry := makeContractCodeEntry(hash, code)
	return ingest.Change{
		Type: xdr.LedgerEntryTypeContractCode,
		Post: &entry,
	}
}

func makeAccountChange() ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeAccount,
		Post: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					AccountId: xdr.MustAddress("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
					Balance:   100,
				},
			},
		},
	}
}

// makeContractInstanceEntry builds a ContractData ledger entry with
// ScvLedgerKeyContractInstance key and a WASM executable (non-SAC).
func makeContractInstanceEntry(contractHash [32]byte, wasmHash xdr.Hash) xdr.LedgerEntry {
	return xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: (*xdr.ContractId)(&contractHash),
				},
				Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
				Durability: xdr.ContractDataDurabilityPersistent,
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvContractInstance,
					Instance: &xdr.ScContractInstance{
						Executable: xdr.ContractExecutable{
							Type:     xdr.ContractExecutableTypeContractExecutableWasm,
							WasmHash: &wasmHash,
						},
					},
				},
			},
		},
	}
}

// makeContractInstanceChange builds an ingest.Change for a ContractData entry with
// ScvLedgerKeyContractInstance key and a WASM executable (non-SAC).
func makeContractInstanceChange(contractHash [32]byte, wasmHash xdr.Hash) ingest.Change {
	entry := makeContractInstanceEntry(contractHash, wasmHash)
	return ingest.Change{
		Type: xdr.LedgerEntryTypeContractData,
		Post: &entry,
	}
}

// makeSACInstanceEntry builds a Stellar-Asset-Contract instance ledger entry. The
// contract ID is the one the asset derives under passphrase, which is what
// sac.AssetFromContractData checks before reporting the entry as a SAC.
func makeSACInstanceEntry(t *testing.T, code, issuer, passphrase string) xdr.LedgerEntry {
	t.Helper()

	asset, err := xdr.NewCreditAsset(code, issuer)
	require.NoError(t, err)
	contractID, err := asset.ContractID(passphrase)
	require.NoError(t, err)
	data, err := sac.AssetToContractData(false, code, issuer, contractID)
	require.NoError(t, err)

	return xdr.LedgerEntry{Data: data}
}

// hotArchiveIterFromEntries builds a hotArchiveIterFactory yielding the given entries,
// standing in for the SDK's hot-archive bucket-list iterator. Called with no entries it
// yields nothing, which is the checkpoint fixture's default.
func hotArchiveIterFromEntries(entries ...xdr.LedgerEntry) hotArchiveIterFactory {
	return func(_ context.Context, _ historyarchive.ArchiveInterface, _ uint32) iter.Seq2[xdr.LedgerEntry, error] {
		return func(yield func(xdr.LedgerEntry, error) bool) {
			for _, entry := range entries {
				if !yield(entry, nil) {
					return
				}
			}
		}
	}
}

// hotArchiveIterError builds a hotArchiveIterFactory whose iterator fails on its first
// yield, the shape a mid-stream bucket read error takes.
func hotArchiveIterError(err error) hotArchiveIterFactory {
	return func(_ context.Context, _ historyarchive.ArchiveInterface, _ uint32) iter.Seq2[xdr.LedgerEntry, error] {
		return func(yield func(xdr.LedgerEntry, error) bool) {
			yield(xdr.LedgerEntry{}, err)
		}
	}
}

// checkpointTestFixture holds a checkpointService and all mocked dependencies.
type checkpointTestFixture struct {
	svc                       *checkpointService
	reader                    *ChangeReaderMock
	archive                   *HistoryArchiveMock
	trustlineAssetModel       *wbdata.TrustlineAssetModelMock
	trustlineBalanceModel     *wbdata.TrustlineBalanceModelMock
	nativeBalanceModel        *wbdata.NativeBalanceModelMock
	sacBalanceModel           *wbdata.SACBalanceModelMock
	liquidityPoolModel        *wbdata.LiquidityPoolModelMock
	liquidityPoolBalanceModel *wbdata.LiquidityPoolBalanceModelMock
	contractModel             *wbdata.ContractModelMock
	protocolWasmModel         *wbdata.ProtocolWasmsModelMock
	protocolContractsModel    *wbdata.ProtocolContractsModelMock
}

// setupCheckpointTest creates a checkpointService with mocked dependencies and a real DB pool.
func setupCheckpointTest(t *testing.T) checkpointTestFixture {
	t.Helper()

	dbt := dbtest.Open(t)
	t.Cleanup(func() { dbt.Close() })
	dbPool, err := db.OpenDBConnectionPool(context.Background(), dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(func() { dbPool.Close() })

	readerMock := NewChangeReaderMock(t)
	trustlineAssetModelMock := wbdata.NewTrustlineAssetModelMock(t)
	trustlineBalanceModelMock := wbdata.NewTrustlineBalanceModelMock(t)
	nativeBalanceModelMock := wbdata.NewNativeBalanceModelMock(t)
	sacBalanceModelMock := wbdata.NewSACBalanceModelMock(t)
	liquidityPoolModelMock := wbdata.NewLiquidityPoolModelMock(t)
	liquidityPoolBalanceModelMock := wbdata.NewLiquidityPoolBalanceModelMock(t)
	// The batch flush always issues a BatchCopy for the liquidity-pool models (with empty slices
	// when a checkpoint carries no pool data), so accept any invocation across all checkpoint tests.
	liquidityPoolModelMock.On("BatchCopy", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	liquidityPoolBalanceModelMock.On("BatchCopy", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	contractModelMock := wbdata.NewContractModelMock(t)
	protocolWasmModelMock := wbdata.NewProtocolWasmsModelMock(t)
	protocolContractsModelMock := wbdata.NewProtocolContractsModelMock(t)

	archiveMock := &HistoryArchiveMock{}

	svc := &checkpointService{
		db:                        dbPool,
		archive:                   archiveMock,
		trustlineAssetModel:       trustlineAssetModelMock,
		trustlineBalanceModel:     trustlineBalanceModelMock,
		nativeBalanceModel:        nativeBalanceModelMock,
		sacBalanceModel:           sacBalanceModelMock,
		liquidityPoolModel:        liquidityPoolModelMock,
		liquidityPoolBalanceModel: liquidityPoolBalanceModelMock,
		contractModel:             contractModelMock,
		protocolWasmModel:         protocolWasmModelMock,
		protocolContractsModel:    protocolContractsModelMock,
		networkPassphrase:         network.TestNetworkPassphrase,
		readerFactory: func(_ context.Context, _ historyarchive.ArchiveInterface, _ uint32) (ingest.ChangeReader, error) {
			return readerMock, nil
		},
		// The hot-archive pass runs on every load, so the factory must be set for every
		// test; an empty iterator keeps tests that only script the live reader unaffected.
		hotArchiveIterFactory: hotArchiveIterFromEntries(),
	}

	return checkpointTestFixture{
		svc:                       svc,
		reader:                    readerMock,
		archive:                   archiveMock,
		trustlineAssetModel:       trustlineAssetModelMock,
		trustlineBalanceModel:     trustlineBalanceModelMock,
		nativeBalanceModel:        nativeBalanceModelMock,
		sacBalanceModel:           sacBalanceModelMock,
		liquidityPoolModel:        liquidityPoolModelMock,
		liquidityPoolBalanceModel: liquidityPoolBalanceModelMock,
		contractModel:             contractModelMock,
		protocolWasmModel:         protocolWasmModelMock,
		protocolContractsModel:    protocolContractsModelMock,
	}
}

func TestCheckpointService_PopulateFromCheckpoint_NilArchive(t *testing.T) {
	svc := &checkpointService{
		archive: nil,
	}

	err := svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.Error(t, err)
	assert.ErrorContains(t, err, "history archive not configured")
}

func TestCheckpointService_PopulateFromCheckpoint_ReaderCreationFails(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbPool, err := db.OpenDBConnectionPool(context.Background(), dbt.DSN)
	require.NoError(t, err)
	defer dbPool.Close()

	svc := &checkpointService{
		db:      dbPool,
		archive: &HistoryArchiveMock{},
		readerFactory: func(_ context.Context, _ historyarchive.ArchiveInterface, _ uint32) (ingest.ChangeReader, error) {
			return nil, errors.New("archive unavailable")
		},
	}

	err = svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.Error(t, err)
	assert.ErrorContains(t, err, "creating checkpoint change reader")
}

func TestCheckpointService_PopulateFromCheckpoint_EmptyCheckpoint(t *testing.T) {
	f := setupCheckpointTest(t)

	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	cursorsCalled := false
	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error {
		cursorsCalled = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, cursorsCalled, "initializeCursors should be called")
}

func TestCheckpointService_PopulateFromCheckpoint_ContractCodeEntry(t *testing.T) {
	f := setupCheckpointTest(t)

	hash := xdr.Hash{1, 2, 3}
	code := []byte{0xDE, 0xAD}
	change := makeContractCodeChange(hash, code)

	// Reader returns one ContractCode then EOF
	f.reader.On("Read").Return(change, nil).Once()
	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	// finalize -> persistProtocolWasms inserts the tracked WASM hash
	f.protocolWasmModel.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

func TestCheckpointService_PopulateFromCheckpoint_AccountEntry(t *testing.T) {
	f := setupCheckpointTest(t)

	accountChange := makeAccountChange()

	f.reader.On("Read").Return(accountChange, nil).Once()
	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	// The batch will flush with 1 native balance
	f.nativeBalanceModel.On("BatchCopy", mock.Anything, mock.Anything,
		mock.MatchedBy(func(b []wbdata.NativeBalance) bool { return len(b) == 1 }),
	).Return(nil).Once()
	f.trustlineBalanceModel.On("BatchCopy", mock.Anything, mock.Anything,
		mock.MatchedBy(func(b []wbdata.TrustlineBalance) bool { return len(b) == 0 }),
	).Return(nil).Once()
	// SAC balances are handled separately in finalize (not via the batch), and only
	// copied when there are verified balances — none here.

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

// makePoolShareChange builds a checkpoint change for a pool_share trustline (per-account shares).
func makePoolShareChange(account xdr.AccountId, poolID xdr.PoolId, shares int64) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeTrustline,
		Post: &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: account,
				Asset:     xdr.TrustLineAsset{Type: xdr.AssetTypeAssetTypePoolShare, LiquidityPoolId: &poolID},
				Balance:   xdr.Int64(shares),
			},
		}},
	}
}

// makeLpEntryChange builds a checkpoint change for a constant-product LiquidityPoolEntry.
func makeLpEntryChange(poolID xdr.PoolId, assetA, assetB xdr.Asset, reserveA, reserveB int64) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeLiquidityPool,
		Post: &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeLiquidityPool,
			LiquidityPool: &xdr.LiquidityPoolEntry{
				LiquidityPoolId: poolID,
				Body: xdr.LiquidityPoolEntryBody{
					Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
					ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
						Params:   xdr.LiquidityPoolConstantProductParameters{AssetA: assetA, AssetB: assetB, Fee: xdr.LiquidityPoolFeeV18},
						ReserveA: xdr.Int64(reserveA),
						ReserveB: xdr.Int64(reserveB),
					},
				},
			},
		}},
	}
}

// makeUnsupportedLpEntryChange builds a checkpoint change for a LiquidityPoolEntry whose
// body is not constant product, the shape liquidity_pools cannot represent.
func makeUnsupportedLpEntryChange(poolID xdr.PoolId) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeLiquidityPool,
		Post: &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeLiquidityPool,
			LiquidityPool: &xdr.LiquidityPoolEntry{
				LiquidityPoolId: poolID,
				Body:            xdr.LiquidityPoolEntryBody{Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct + 1},
			},
		}},
	}
}

func TestCheckpointService_PopulateFromCheckpoint_LiquidityPoolEntries(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbPool, err := db.OpenDBConnectionPool(context.Background(), dbt.DSN)
	require.NoError(t, err)
	defer dbPool.Close()

	issuer := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
	account := xdr.MustAddress(issuer)
	poolID := xdr.PoolId{1, 2, 3}
	expectedPoolID := xdr.Hash(poolID).HexString()
	usdc := xdr.MustNewCreditAsset("USDC", issuer)

	readerMock := NewChangeReaderMock(t)
	readerMock.On("Read").Return(makeLpEntryChange(poolID, xdr.MustNewNativeAsset(), usdc, 100, 200), nil).Once()
	readerMock.On("Read").Return(makePoolShareChange(account, poolID, 5000), nil).Once()
	readerMock.On("Read").Return(ingest.Change{}, io.EOF).Once()
	readerMock.On("Close").Return(nil).Once()

	trustlineBalanceModel := wbdata.NewTrustlineBalanceModelMock(t)
	nativeBalanceModel := wbdata.NewNativeBalanceModelMock(t)
	sacBalanceModel := wbdata.NewSACBalanceModelMock(t)
	lpModel := wbdata.NewLiquidityPoolModelMock(t)
	lpBalanceModel := wbdata.NewLiquidityPoolBalanceModelMock(t)

	// The pool-share trustline and LP entry route to the liquidity-pool models, never to
	// trustline/native/sac (those flush empty).
	trustlineBalanceModel.On("BatchCopy", mock.Anything, mock.Anything, mock.MatchedBy(func(b []wbdata.TrustlineBalance) bool { return len(b) == 0 })).Return(nil).Once()
	nativeBalanceModel.On("BatchCopy", mock.Anything, mock.Anything, mock.MatchedBy(func(b []wbdata.NativeBalance) bool { return len(b) == 0 })).Return(nil).Once()
	lpModel.On("BatchCopy", mock.Anything, mock.Anything, mock.MatchedBy(func(pools []wbdata.LiquidityPool) bool {
		return len(pools) == 1 && pools[0].PoolID == expectedPoolID &&
			pools[0].AssetA == "native" && pools[0].AmountA == 100 &&
			pools[0].AssetB == "USDC:"+issuer && pools[0].AmountB == 200
	})).Return(nil).Once()
	lpBalanceModel.On("BatchCopy", mock.Anything, mock.Anything, mock.MatchedBy(func(bals []wbdata.LiquidityPoolBalance) bool {
		return len(bals) == 1 && bals[0].PoolID == expectedPoolID &&
			bals[0].Shares == 5000 && bals[0].AccountID == types.AddressBytea(issuer)
	})).Return(nil).Once()

	svc := &checkpointService{
		db:                        dbPool,
		archive:                   &HistoryArchiveMock{},
		trustlineBalanceModel:     trustlineBalanceModel,
		nativeBalanceModel:        nativeBalanceModel,
		sacBalanceModel:           sacBalanceModel,
		liquidityPoolModel:        lpModel,
		liquidityPoolBalanceModel: lpBalanceModel,
		networkPassphrase:         network.TestNetworkPassphrase,
		readerFactory: func(_ context.Context, _ historyarchive.ArchiveInterface, _ uint32) (ingest.ChangeReader, error) {
			return readerMock, nil
		},
		hotArchiveIterFactory: hotArchiveIterFromEntries(),
	}

	err = svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

func TestCheckpointService_PopulateFromCheckpoint_UnsupportedLiquidityPoolBodyFails(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()
	dbPool, err := db.OpenDBConnectionPool(context.Background(), dbt.DSN)
	require.NoError(t, err)
	defer dbPool.Close()

	poolID := xdr.PoolId{1, 2, 3}

	// LiquidityPoolType admits only constant product, so the decoder rejects any
	// other discriminant and this shape cannot come off an archive. Constructed
	// here to pin the behaviour if that ever changes: liquidity_pool_balances
	// references liquidity_pools, so a pool the load cannot represent must abort
	// it rather than silently drop every holder's shares.
	readerMock := NewChangeReaderMock(t)
	readerMock.On("Read").Return(makeUnsupportedLpEntryChange(poolID), nil).Once()
	readerMock.On("Close").Return(nil).Once()

	svc := &checkpointService{
		db:                        dbPool,
		archive:                   &HistoryArchiveMock{},
		trustlineBalanceModel:     wbdata.NewTrustlineBalanceModelMock(t),
		nativeBalanceModel:        wbdata.NewNativeBalanceModelMock(t),
		sacBalanceModel:           wbdata.NewSACBalanceModelMock(t),
		liquidityPoolModel:        wbdata.NewLiquidityPoolModelMock(t),
		liquidityPoolBalanceModel: wbdata.NewLiquidityPoolBalanceModelMock(t),
		networkPassphrase:         network.TestNetworkPassphrase,
		readerFactory: func(_ context.Context, _ historyarchive.ArchiveInterface, _ uint32) (ingest.ChangeReader, error) {
			return readerMock, nil
		},
		hotArchiveIterFactory: hotArchiveIterFromEntries(),
	}

	err = svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "liquidity_pools cannot represent")
	assert.Contains(t, err.Error(), xdr.Hash(poolID).HexString())
}

func TestCheckpointService_PopulateFromCheckpoint_ContractDataEntry(t *testing.T) {
	f := setupCheckpointTest(t)

	contractHash := [32]byte{10, 20, 30}
	wasmHash := xdr.Hash{40, 50, 60}
	contractDataChange := makeContractInstanceChange(contractHash, wasmHash)

	f.reader.On("Read").Return(contractDataChange, nil).Once()
	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	// Flush remaining batch (empty balances but 0 entries)
	f.trustlineBalanceModel.On("BatchCopy", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	f.nativeBalanceModel.On("BatchCopy", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	f.sacBalanceModel.On("BatchCopy", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Finalize: contract model
	f.contractModel.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Protocol WASM + contracts: we tracked contract data but no contract code hash matched
	f.protocolWasmModel.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	f.protocolContractsModel.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

// makeSACInstanceChange builds a verified SAC contract-instance entry for the
// given classic asset. The contract ID is the deterministically derived Stellar
// Asset Contract ID for that asset, so sac.AssetFromContractData authenticates it
// — this is what identifies a contract as a SAC rather than a shape look-alike.
func makeSACInstanceChange(t *testing.T, code, issuer, passphrase string) (ingest.Change, [32]byte) {
	t.Helper()
	asset := xdr.MustNewCreditAsset(code, issuer)
	contractID, err := asset.ContractID(passphrase)
	require.NoError(t, err)
	data, err := sac.AssetToContractData(false, code, issuer, contractID)
	require.NoError(t, err)
	return ingest.Change{
		Type: xdr.LedgerEntryTypeContractData,
		Post: &xdr.LedgerEntry{Data: data},
	}, contractID
}

// TestCheckpointService_PopulateFromCheckpoint_HotArchiveWasmPersisted proves contract
// code evicted to the hot archive still reaches protocol_wasms. The live reader sees
// nothing; the hash comes from the hot-archive pass alone.
func TestCheckpointService_PopulateFromCheckpoint_HotArchiveWasmPersisted(t *testing.T) {
	f := setupCheckpointTest(t)

	wasmHash := xdr.Hash{0xA1, 0xB2, 0xC3}
	f.svc.hotArchiveIterFactory = hotArchiveIterFromEntries(makeContractCodeEntry(wasmHash, []byte{0xDE, 0xAD}))

	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	f.protocolWasmModel.On("BatchInsert", mock.Anything, mock.Anything,
		mock.MatchedBy(func(wasms []wbdata.ProtocolWasms) bool {
			return len(wasms) == 1 && wasms[0].WasmHash == types.HashBytea(hex.EncodeToString(wasmHash[:]))
		}),
	).Return(nil).Once()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

// TestCheckpointService_PopulateFromCheckpoint_HotArchiveWasmUnblocksLiveInstance covers
// the exact gap the hot-archive pass closes: a live contract instance whose code was
// evicted. Without the archived code entry, persistProtocolContracts drops the mapping
// (the contracts_with_missing_wasm_skipped subtest documents that fallback).
func TestCheckpointService_PopulateFromCheckpoint_HotArchiveWasmUnblocksLiveInstance(t *testing.T) {
	f := setupCheckpointTest(t)

	contractHash := [32]byte{0x0A, 0x0B, 0x0C}
	wasmHash := xdr.Hash{0x1A, 0x2B, 0x3C}
	contractAddr := strkey.MustEncode(strkey.VersionByteContract, contractHash[:])

	f.reader.On("Read").Return(makeContractInstanceChange(contractHash, wasmHash), nil).Once()
	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	f.svc.hotArchiveIterFactory = hotArchiveIterFromEntries(makeContractCodeEntry(wasmHash, []byte{0xBE, 0xEF}))

	f.contractModel.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(cs []*wbdata.Contract) bool {
		return len(cs) == 1 && cs[0].ContractID == contractAddr
	})).Return(nil).Once()
	f.protocolWasmModel.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(wasms []wbdata.ProtocolWasms) bool {
		return len(wasms) == 1 && wasms[0].WasmHash == types.HashBytea(hex.EncodeToString(wasmHash[:]))
	})).Return(nil).Once()
	f.protocolContractsModel.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(cs []wbdata.ProtocolContracts) bool {
		return len(cs) == 1 &&
			cs[0].ContractID == types.HashBytea(hex.EncodeToString(contractHash[:])) &&
			cs[0].WasmHash == types.HashBytea(hex.EncodeToString(wasmHash[:]))
	})).Return(nil).Once()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

// TestCheckpointService_PopulateFromCheckpoint_HotArchiveInstanceIngested proves an
// archived wasm-executable instance lands in both contract_tokens and protocol_contracts
// even though the live reader never sees it.
func TestCheckpointService_PopulateFromCheckpoint_HotArchiveInstanceIngested(t *testing.T) {
	f := setupCheckpointTest(t)

	contractHash := [32]byte{0x21, 0x22, 0x23}
	wasmHash := xdr.Hash{0x31, 0x32, 0x33}
	contractAddr := strkey.MustEncode(strkey.VersionByteContract, contractHash[:])

	// The instance is yielded before its code: the mapping is recorded first and is only
	// matched against wasmClassifications in finalize, after the whole pass has run.
	f.svc.hotArchiveIterFactory = hotArchiveIterFromEntries(
		makeContractInstanceEntry(contractHash, wasmHash),
		makeContractCodeEntry(wasmHash, []byte{0xFE, 0xED}),
	)

	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	f.contractModel.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(cs []*wbdata.Contract) bool {
		return len(cs) == 1 && cs[0].ID == wbdata.DeterministicContractID(contractAddr) &&
			cs[0].ContractID == contractAddr && cs[0].Type == string(types.ContractTypeUnknown)
	})).Return(nil).Once()
	f.protocolWasmModel.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(wasms []wbdata.ProtocolWasms) bool {
		return len(wasms) == 1 && wasms[0].WasmHash == types.HashBytea(hex.EncodeToString(wasmHash[:]))
	})).Return(nil).Once()
	f.protocolContractsModel.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(cs []wbdata.ProtocolContracts) bool {
		return len(cs) == 1 &&
			cs[0].ContractID == types.HashBytea(hex.EncodeToString(contractHash[:])) &&
			cs[0].WasmHash == types.HashBytea(hex.EncodeToString(wasmHash[:]))
	})).Return(nil).Once()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

// TestCheckpointService_PopulateFromCheckpoint_HotArchiveBalanceKeySkipped proves archived
// Balance-key entries are ignored: live ingestion recreates their rows when the entries
// are restored, so the hot-archive pass must not write balances or contract_tokens.
func TestCheckpointService_PopulateFromCheckpoint_HotArchiveBalanceKeySkipped(t *testing.T) {
	f := setupCheckpointTest(t)

	holder := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
	balanceEntry := *makeContractBalanceChange([32]byte{0x41, 0x42, 0x43}, holder).Post
	f.svc.hotArchiveIterFactory = hotArchiveIterFromEntries(balanceEntry)

	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)

	f.sacBalanceModel.AssertNotCalled(t, "BatchCopy", mock.Anything, mock.Anything, mock.Anything)
	f.contractModel.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
}

// TestCheckpointService_PopulateFromCheckpoint_HotArchiveSACInstanceSkipped proves archived
// SAC instances are ignored. A SAC row would otherwise be queued for RPC metadata
// enrichment for a contract that is not even live.
func TestCheckpointService_PopulateFromCheckpoint_HotArchiveSACInstanceSkipped(t *testing.T) {
	f := setupCheckpointTest(t)

	issuer := "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	sacEntry := makeSACInstanceEntry(t, "USDC", issuer, f.svc.networkPassphrase)

	// Fixture guard: a broken entry would also produce zero rows, so assert the
	// entry really takes the SAC branch — the one processArchivedContractData skips.
	contractData := sacEntry.Data.MustContractData()
	result := f.svc.processContractInstanceChange(
		ingest.Change{Type: sacEntry.Data.Type, Post: &sacEntry},
		strkey.MustEncode(strkey.VersionByteContract, contractData.Contract.ContractId[:]),
		contractData,
	)
	require.True(t, result.IsSAC, "fixture entry must be recognized as a SAC instance")

	f.svc.hotArchiveIterFactory = hotArchiveIterFromEntries(sacEntry)

	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)

	f.contractModel.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
}

// TestCheckpointService_PopulateFromCheckpoint_HotArchivePreP23Skipped exercises the real
// factory against a pre-protocol-23 checkpoint, which has no hot-archive bucket list. The
// archive mock is stubbed for GetCheckpointHAS alone, so any attempt to open a bucket
// after the version check would fail this test on an unexpected call.
func TestCheckpointService_PopulateFromCheckpoint_HotArchivePreP23Skipped(t *testing.T) {
	f := setupCheckpointTest(t)

	f.svc.hotArchiveIterFactory = defaultHotArchiveIterFactory
	f.archive.On("GetCheckpointHAS", uint32(100)).
		Return(historyarchive.HistoryArchiveState{Version: 1}, nil).Once()

	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
	f.archive.AssertExpectations(t)
}

// TestCheckpointService_PopulateFromCheckpoint_HotArchiveIteratorErrorAborts proves a
// failed hot-archive read aborts the load transaction rather than committing a checkpoint
// that silently omits archived entries.
func TestCheckpointService_PopulateFromCheckpoint_HotArchiveIteratorErrorAborts(t *testing.T) {
	f := setupCheckpointTest(t)

	iterErr := errors.New("bucket stream truncated")
	f.svc.hotArchiveIterFactory = hotArchiveIterError(iterErr)

	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	cursorsCalled := false
	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error {
		cursorsCalled = true
		return nil
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "reading hot archive entries")
	assert.ErrorIs(t, err, iterErr)

	// The pass runs inside the load transaction and before finalize, so neither the
	// finalize-stage writes nor cursor initialization may have run.
	assert.False(t, cursorsCalled)
	f.contractModel.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
	f.protocolWasmModel.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
	f.protocolContractsModel.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
}

func TestDefaultHotArchiveIterFactory(t *testing.T) {
	ctx := context.Background()

	t.Run("has_error", func(t *testing.T) {
		archive := &HistoryArchiveMock{}
		hasErr := errors.New("archive unreachable")
		archive.On("GetCheckpointHAS", uint32(100)).
			Return(historyarchive.HistoryArchiveState{}, hasErr).Once()

		var errs []error
		for _, iterErr := range defaultHotArchiveIterFactory(ctx, archive, 100) {
			errs = append(errs, iterErr)
		}

		// The failure surfaces as a single yielded error rather than a panic or a silently
		// empty pass, which is what lets processHotArchive abort the load.
		require.Len(t, errs, 1)
		require.Error(t, errs[0])
		assert.ErrorContains(t, errs[0], "getting checkpoint HAS for hot archive at ledger 100")
		assert.ErrorIs(t, errs[0], hasErr)
		archive.AssertExpectations(t)
	})

	t.Run("pre_p23_skips", func(t *testing.T) {
		archive := &HistoryArchiveMock{}
		archive.On("GetCheckpointHAS", uint32(100)).
			Return(historyarchive.HistoryArchiveState{Version: 1}, nil).Once()

		count := 0
		for range defaultHotArchiveIterFactory(ctx, archive, 100) {
			count++
		}

		assert.Zero(t, count)
		archive.AssertExpectations(t)
	})
}

// makeSACBalanceChange builds an ingest.Change for a ContractData Balance
// entry whose holder is itself a contract — the shape
// sac.ContractBalanceFromContractData requires to recognize a SAC balance.
// The shape alone does not identify the contract as a SAC; the checkpoint
// records such a balance only when the contract is also confirmed via its
// instance entry (see makeSACInstanceChange).
func makeSACBalanceChange(tokenContractHash, holderContractHash [32]byte) ingest.Change {
	return ingest.Change{
		Type: xdr.LedgerEntryTypeContractData,
		Post: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeContractData,
				ContractData: &xdr.ContractDataEntry{
					Contract: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: (*xdr.ContractId)(&tokenContractHash),
					},
					Key: xdr.ScVal{
						Type: xdr.ScValTypeScvVec,
						Vec: ptrToScVec([]xdr.ScVal{
							{Type: xdr.ScValTypeScvSymbol, Sym: ptrToScSymbol("Balance")},
							{
								Type: xdr.ScValTypeScvAddress,
								Address: &xdr.ScAddress{
									Type:       xdr.ScAddressTypeScAddressTypeContract,
									ContractId: (*xdr.ContractId)(&holderContractHash),
								},
							},
						}),
					},
					Durability: xdr.ContractDataDurabilityPersistent,
					Val:        makeBalanceMapVal(500, true, false),
				},
			},
		},
	}
}

// TestCheckpointService_PopulateFromCheckpoint_VerifiedSACBalanceRecorded proves a SAC
// balance is recorded when its contract is confirmed as a SAC via
// its instance entry. The instance carries the asset info, so contract_tokens is written
// with type=SAC and code/issuer set — no RPC enrichment is needed.
func TestCheckpointService_PopulateFromCheckpoint_VerifiedSACBalanceRecorded(t *testing.T) {
	f := setupCheckpointTest(t)

	issuer := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
	instanceChange, contractID := makeSACInstanceChange(t, "USDC", issuer, f.svc.networkPassphrase)
	contractAddr := strkey.MustEncode(strkey.VersionByteContract, contractID[:])
	holderHash := [32]byte{8, 8, 8}
	balanceChange := makeSACBalanceChange(contractID, holderHash)

	f.reader.On("Read").Return(balanceChange, nil).Once()
	f.reader.On("Read").Return(instanceChange, nil).Once()
	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	// The verified SAC balance is recorded in finalize (trustline/native/lp batch is empty).
	f.sacBalanceModel.On("BatchCopy", mock.Anything, mock.Anything, mock.MatchedBy(func(b []wbdata.SACBalance) bool {
		return len(b) == 1 && b[0].ContractID == wbdata.DeterministicContractID(contractAddr)
	})).Return(nil).Once()
	// contract_tokens is written from the instance with code/issuer already set (no enrichment).
	f.contractModel.On("BatchInsert", mock.Anything, mock.Anything, mock.MatchedBy(func(cs []*wbdata.Contract) bool {
		return len(cs) == 1 && cs[0].ContractID == contractAddr && cs[0].Type == string(types.ContractTypeSAC) &&
			cs[0].Code != nil && *cs[0].Code == "USDC"
	})).Return(nil).Once()

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

// TestCheckpointService_PopulateFromCheckpoint_UnverifiedSACBalanceDropped verifies that a
// Balance-shaped contract-data entry whose contract is NOT confirmed as a SAC (no instance
// entry in the checkpoint) is dropped: no balance is recorded, no contract_tokens row is
// created from the shape, and no RPC enrichment runs. Recording it would associate a balance
// with a contract of unknown type and, in live ingestion, violate the deferred
// fk_contract_token at COMMIT.
func TestCheckpointService_PopulateFromCheckpoint_UnverifiedSACBalanceDropped(t *testing.T) {
	f := setupCheckpointTest(t)

	tokenHash := [32]byte{9, 9, 9}
	holderHash := [32]byte{8, 8, 8}
	change := makeSACBalanceChange(tokenHash, holderHash)

	f.reader.On("Read").Return(change, nil).Once()
	f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
	f.reader.On("Close").Return(nil).Once()

	// The unverified balance is dropped, so nothing is written: no SAC BatchCopy (finalize
	// skips the empty set) and no contractModel.BatchInsert from the shape. The strict
	// mocks fail the test on any such unexpected call.

	err := f.svc.PopulateFromCheckpoint(context.Background(), 100, func(_ pgx.Tx) error { return nil })
	require.NoError(t, err)
}

func TestCheckpointService_PopulateFromCheckpoint_ErrorPropagation(t *testing.T) {
	tests := []struct {
		name           string
		setupMocks     func(f *checkpointTestFixture) func(pgx.Tx) error
		expectedErrMsg string
	}{
		{
			name: "reader_read_error",
			setupMocks: func(f *checkpointTestFixture) func(pgx.Tx) error {
				f.reader.On("Read").Return(ingest.Change{}, errors.New("network timeout")).Once()
				f.reader.On("Close").Return(nil).Once()
				return func(_ pgx.Tx) error { return nil }
			},
			expectedErrMsg: "reading checkpoint changes",
		},
		{
			name: "initialize_cursors_error",
			setupMocks: func(f *checkpointTestFixture) func(pgx.Tx) error {
				f.reader.On("Read").Return(ingest.Change{}, io.EOF).Once()
				f.reader.On("Close").Return(nil).Once()
				return func(_ pgx.Tx) error { return errors.New("cursor init failed") }
			},
			expectedErrMsg: "initializing cursors",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := setupCheckpointTest(t)
			initializeCursors := tt.setupMocks(&f)

			err := f.svc.PopulateFromCheckpoint(context.Background(), 100, initializeCursors)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.expectedErrMsg)
		})
	}
}

func TestCheckpointService_PopulateFromCheckpoint_ContextCancellation(t *testing.T) {
	f := setupCheckpointTest(t)

	ctx, cancel := context.WithCancel(context.Background())

	f.reader.On("Close").Return(nil).Once()

	// First Read succeeds but cancels the context; the next loop iteration detects cancellation
	f.reader.On("Read").Run(func(_ mock.Arguments) {
		cancel()
	}).Return(makeAccountChange(), nil).Once()

	err := f.svc.PopulateFromCheckpoint(ctx, 100, func(_ pgx.Tx) error { return nil })
	require.Error(t, err)
	assert.ErrorContains(t, err, "checkpoint processing cancelled")
}

// Tests ported from wasm_ingestion_test.go

func TestCheckpointProcessor_ProcessContractCode(t *testing.T) {
	ctx := context.Background()

	t.Run("tracks_hash", func(t *testing.T) {
		proc := &checkpointProcessor{
			data:                        newCheckpointData(),
			wasmClassifications:         make(map[xdr.Hash]types.ContractType),
			contractAddressesByWasmHash: make(map[xdr.Hash][]xdr.Hash),
		}

		hash := xdr.Hash{1, 2, 3}
		code := []byte{0xDE, 0xAD}

		proc.processContractCode(ctx, hash, code)

		// WASM hash tracked
		_, tracked := proc.wasmClassifications[hash]
		assert.True(t, tracked, "hash should be tracked in wasmClassifications")
	})

	t.Run("duplicate_hash_deduplicated", func(t *testing.T) {
		proc := &checkpointProcessor{
			data:                        newCheckpointData(),
			wasmClassifications:         make(map[xdr.Hash]types.ContractType),
			contractAddressesByWasmHash: make(map[xdr.Hash][]xdr.Hash),
		}

		hash := xdr.Hash{1, 2, 3}
		code := []byte{0xDE, 0xAD}

		proc.processContractCode(ctx, hash, code)
		proc.processContractCode(ctx, hash, code)

		assert.Len(t, proc.wasmClassifications, 1, "duplicate hash should be deduplicated in map")
	})
}

func TestCheckpointService_PersistProtocolWasms(t *testing.T) {
	ctx := context.Background()

	t.Run("no_hashes_skips_insert", func(t *testing.T) {
		protocolWasmModelMock := wbdata.NewProtocolWasmsModelMock(t)
		svc := &checkpointService{protocolWasmModel: protocolWasmModelMock}

		err := svc.persistProtocolWasms(ctx, nil, map[xdr.Hash]types.ContractType{})
		require.NoError(t, err)
		protocolWasmModelMock.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("single_hash_persisted", func(t *testing.T) {
		protocolWasmModelMock := wbdata.NewProtocolWasmsModelMock(t)
		svc := &checkpointService{protocolWasmModel: protocolWasmModelMock}

		hash := xdr.Hash{10, 20, 30}
		wasmClassifications := map[xdr.Hash]types.ContractType{hash: types.ContractTypeUnknown}

		protocolWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(wasms []wbdata.ProtocolWasms) bool {
				if len(wasms) != 1 {
					return false
				}
				return wasms[0].WasmHash == types.HashBytea(hex.EncodeToString(hash[:])) && wasms[0].ProtocolID == nil
			}),
		).Return(nil).Once()

		err := svc.persistProtocolWasms(ctx, nil, wasmClassifications)
		require.NoError(t, err)
	})

	t.Run("batch_insert_error_propagated", func(t *testing.T) {
		protocolWasmModelMock := wbdata.NewProtocolWasmsModelMock(t)
		svc := &checkpointService{protocolWasmModel: protocolWasmModelMock}

		hash := xdr.Hash{99}
		wasmClassifications := map[xdr.Hash]types.ContractType{hash: types.ContractTypeUnknown}
		insertErr := errors.New("db connection lost")

		protocolWasmModelMock.On("BatchInsert", mock.Anything, mock.Anything, mock.Anything).
			Return(insertErr).Once()

		err := svc.persistProtocolWasms(ctx, nil, wasmClassifications)
		require.Error(t, err)
		assert.ErrorContains(t, err, "persisting protocol wasms")
		assert.ErrorIs(t, err, insertErr)
	})
}

func TestCheckpointService_PersistProtocolContracts(t *testing.T) {
	ctx := context.Background()

	t.Run("empty_no_op", func(t *testing.T) {
		protocolContractsModelMock := wbdata.NewProtocolContractsModelMock(t)
		svc := &checkpointService{protocolContractsModel: protocolContractsModelMock}

		err := svc.persistProtocolContracts(ctx, nil, map[xdr.Hash]types.ContractType{}, map[xdr.Hash][]xdr.Hash{})
		require.NoError(t, err)
		protocolContractsModelMock.AssertNotCalled(t, "BatchInsert", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("single_contract", func(t *testing.T) {
		protocolContractsModelMock := wbdata.NewProtocolContractsModelMock(t)
		svc := &checkpointService{protocolContractsModel: protocolContractsModelMock}

		contractHash := [32]byte{10, 20, 30}
		wasmHash := xdr.Hash{40, 50, 60}
		wasmClassifications := map[xdr.Hash]types.ContractType{wasmHash: types.ContractTypeUnknown}
		contractAddressesByWasmHash := map[xdr.Hash][]xdr.Hash{
			wasmHash: {xdr.Hash(contractHash)},
		}

		protocolContractsModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(contracts []wbdata.ProtocolContracts) bool {
				if len(contracts) != 1 {
					return false
				}
				return contracts[0].ContractID == types.HashBytea(hex.EncodeToString(contractHash[:])) &&
					contracts[0].WasmHash == types.HashBytea(hex.EncodeToString(wasmHash[:])) &&
					contracts[0].Name == nil
			}),
		).Return(nil).Once()

		err := svc.persistProtocolContracts(ctx, nil, wasmClassifications, contractAddressesByWasmHash)
		require.NoError(t, err)
	})

	t.Run("contracts_with_missing_wasm_skipped", func(t *testing.T) {
		protocolContractsModelMock := wbdata.NewProtocolContractsModelMock(t)
		svc := &checkpointService{protocolContractsModel: protocolContractsModelMock}

		knownWasm := xdr.Hash{1}
		unknownWasm := xdr.Hash{2}
		contractHash1 := [32]byte{10}
		contractHash2 := [32]byte{20}

		wasmClassifications := map[xdr.Hash]types.ContractType{knownWasm: types.ContractTypeUnknown}
		contractAddressesByWasmHash := map[xdr.Hash][]xdr.Hash{
			knownWasm:   {xdr.Hash(contractHash1)},
			unknownWasm: {xdr.Hash(contractHash2)},
		}

		protocolContractsModelMock.On("BatchInsert", mock.Anything, mock.Anything,
			mock.MatchedBy(func(contracts []wbdata.ProtocolContracts) bool {
				return len(contracts) == 1 && contracts[0].WasmHash == types.HashBytea(hex.EncodeToString(knownWasm[:]))
			}),
		).Return(nil).Once()

		err := svc.persistProtocolContracts(ctx, nil, wasmClassifications, contractAddressesByWasmHash)
		require.NoError(t, err)
	})
}

// Tests ported from token_ingestion_test.go for checkpoint-specific logic

func TestCheckpointProcessor_ProcessEntry(t *testing.T) {
	// newTestCheckpointProcessor creates a checkpointProcessor with minimal deps for unit testing.
	newTestCheckpointProcessor := func() *checkpointProcessor {
		svc := &checkpointService{networkPassphrase: network.TestNetworkPassphrase}
		return &checkpointProcessor{
			service:                     svc,
			checkpointLedger:            100,
			data:                        newCheckpointData(),
			wasmClassifications:         make(map[xdr.Hash]types.ContractType),
			contractAddressesByWasmHash: make(map[xdr.Hash][]xdr.Hash),
			batch: &batch{
				nativeBalances:    make([]wbdata.NativeBalance, 0),
				trustlineBalances: make([]wbdata.TrustlineBalance, 0),
			},
		}
	}

	t.Run("account_entry", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		change := makeAccountChangeWithBalance(address, 100_000_000, 3, 5_000_000, 2_000_000)
		require.NoError(t, proc.processEntry(change))

		require.Len(t, proc.batch.nativeBalances, 1)
		nb := proc.batch.nativeBalances[0]
		assert.Equal(t, address, string(nb.AccountID))
		assert.Equal(t, int64(100_000_000), nb.Balance)
		assert.Equal(t, uint32(100), nb.LedgerNumber)
		assert.Equal(t, 1, proc.entries)
		assert.Equal(t, 1, proc.accountCount)
	})

	t.Run("trustline_entry", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		issuer := "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
		assetCode := "USDC"

		change := makeTrustlineChange(address, assetCode, issuer, 5_000_000, 100_000_000)
		require.NoError(t, proc.processEntry(change))

		require.Len(t, proc.batch.trustlineBalances, 1)
		tb := proc.batch.trustlineBalances[0]
		assert.Equal(t, address, string(tb.AccountID))
		assert.Equal(t, wbdata.DeterministicAssetID(assetCode, issuer), tb.AssetID)
		assert.Equal(t, 1, proc.entries)
		assert.Equal(t, 1, proc.trustlineCount)
	})

	t.Run("trustline_pool_share_routed_to_liquidity_pool_balances", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		address := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"
		change := makePoolShareTrustlineChange(address)
		require.NoError(t, proc.processEntry(change))

		// Pool-share trustlines are shares, not asset balances.
		assert.Empty(t, proc.batch.trustlineBalances)
		require.Len(t, proc.batch.liquidityPoolBalances, 1)
		share := proc.batch.liquidityPoolBalances[0]
		assert.Equal(t, address, string(share.AccountID))
		assert.Equal(t, xdr.Hash(xdr.PoolId{1, 2, 3}).HexString(), share.PoolID)
		assert.Equal(t, int64(1000), share.Shares)
		assert.Equal(t, uint32(100), share.LedgerNumber)
		assert.Equal(t, 1, proc.entries)
	})

	t.Run("liquidity_pool_entry_routed_to_liquidity_pools", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		issuer := "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
		change := makeLpEntryChange(xdr.PoolId{4, 5, 6}, xdr.MustNewNativeAsset(), xdr.MustNewCreditAsset("USDC", issuer), 100, 200)
		require.NoError(t, proc.processEntry(change))

		require.Len(t, proc.batch.liquidityPools, 1)
		lp := proc.batch.liquidityPools[0]
		assert.Equal(t, xdr.Hash(xdr.PoolId{4, 5, 6}).HexString(), lp.PoolID)
		assert.Equal(t, "native", lp.AssetA)
		assert.Equal(t, int64(100), lp.AmountA)
		assert.Equal(t, "USDC:"+issuer, lp.AssetB)
		assert.Equal(t, int64(200), lp.AmountB)
		assert.Equal(t, 1, proc.entries)
	})

	t.Run("contract_instance_non_sac", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		contractHash := [32]byte{0xAA, 0xBB, 0xCC}
		wasmHash := xdr.Hash{0x11, 0x22, 0x33}

		change := makeContractInstanceChange(contractHash, wasmHash)
		require.NoError(t, proc.processEntry(change))

		contractAddr := strkey.MustEncode(strkey.VersionByteContract, contractHash[:])
		contractUUID := wbdata.DeterministicContractID(contractAddr)

		require.Contains(t, proc.data.uniqueContractTokens, contractUUID)
		contract := proc.data.uniqueContractTokens[contractUUID]
		assert.Equal(t, contractAddr, contract.ContractID)
		assert.Equal(t, string(types.ContractTypeUnknown), contract.Type)

		// Tracked for protocol contracts
		require.Contains(t, proc.contractAddressesByWasmHash, wasmHash)

		assert.Equal(t, 1, proc.entries)
	})

	t.Run("contract_balance_non_sac_skipped", func(t *testing.T) {
		proc := newTestCheckpointProcessor()
		contractHash := [32]byte{0xDD, 0xEE, 0xFF}
		holderAddress := "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"

		change := makeContractBalanceChange(contractHash, holderAddress)
		require.NoError(t, proc.processEntry(change))

		// Non-SAC balance entries are no longer tracked (SEP-41 tracking removed)
		assert.Equal(t, 0, proc.entries)
		assert.Empty(t, proc.pendingSACBalances)
	})

	t.Run("unhandled_entry_type_ignored", func(t *testing.T) {
		proc := newTestCheckpointProcessor()

		change := ingest.Change{
			Type: xdr.LedgerEntryTypeOffer,
			Post: &xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeOffer,
					Offer: &xdr.OfferEntry{
						SellerId: xdr.MustAddress("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
						OfferId:  1,
					},
				},
			},
		}
		require.NoError(t, proc.processEntry(change))

		assert.Equal(t, 0, proc.entries)
	})
}

func TestCheckpointService_ExtractHolderAddress(t *testing.T) {
	service := &checkpointService{}

	tests := []struct {
		name    string
		key     xdr.ScVal
		want    string
		wantErr bool
	}{
		{
			name: "valid balance entry",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{
						Type: xdr.ScValTypeScvSymbol,
						Sym:  ptrToScSymbol("Balance"),
					},
					{
						Type: xdr.ScValTypeScvAddress,
						Address: &xdr.ScAddress{
							Type:      xdr.ScAddressTypeScAddressTypeAccount,
							AccountId: ptrToAccountID("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
						},
					},
				}),
			},
			want:    "GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N",
			wantErr: false,
		},
		{
			name: "not a vector",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvU32,
				U32:  ptrToUint32(123),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "wrong vector length",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{Type: xdr.ScValTypeScvSymbol, Sym: ptrToScSymbol("Balance")},
				}),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "wrong symbol",
			key: xdr.ScVal{
				Type: xdr.ScValTypeScvVec,
				Vec: ptrToScVec([]xdr.ScVal{
					{Type: xdr.ScValTypeScvSymbol, Sym: ptrToScSymbol("NotBalance")},
					{
						Type: xdr.ScValTypeScvAddress,
						Address: &xdr.ScAddress{
							Type:      xdr.ScAddressTypeScAddressTypeAccount,
							AccountId: ptrToAccountID("GAFOZZL77R57WMGES6BO6WJDEIFJ6662GMCVEX6ZESULRX3FRBGSSV5N"),
						},
					},
				}),
			},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := service.extractHolderAddress(tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Empty(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// processContractInstanceChange reads Val as a contract instance, but the
// caller constrains only Key.Type. Key.Type and Val.Type are independent XDR
// unions, so a mismatched Val must skip rather than panic the ingest
// goroutine. Protocol 28 also adds an external-ref executable that carries no
// WASM hash and so cannot be classified.
func TestCheckpointService_ProcessContractInstanceChange(t *testing.T) {
	ingestionMetrics := metrics.NewMetrics(prometheus.NewRegistry()).Ingestion
	svc := &checkpointService{
		networkPassphrase: network.TestNetworkPassphrase,
		metricsService:    ingestionMetrics,
	}

	contractID := xdr.ContractId{0x01, 0x02, 0x03}
	ownerID := xdr.ContractId{0x09, 0x08, 0x07}
	contractAddress := strkey.MustEncode(strkey.VersionByteContract, contractID[:])
	wasmHash := xdr.Hash{0xaa, 0xbb, 0xcc}

	entryFor := func(val xdr.ScVal) xdr.ContractDataEntry {
		return xdr.ContractDataEntry{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &contractID,
			},
			Key: xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
			Val: val,
		}
	}
	instanceVal := func(executable xdr.ContractExecutable) xdr.ScVal {
		return xdr.ScVal{
			Type:     xdr.ScValTypeScvContractInstance,
			Instance: &xdr.ScContractInstance{Executable: executable},
		}
	}
	u32 := xdr.Uint32(7)

	tests := []struct {
		name           string
		val            xdr.ScVal
		expectSkip     bool
		expectedWasm   *xdr.Hash
		expectExternal bool
	}{
		{
			name: "wasm executable is recorded",
			val: instanceVal(xdr.ContractExecutable{
				Type:     xdr.ContractExecutableTypeContractExecutableWasm,
				WasmHash: &wasmHash,
			}),
			expectSkip:   false,
			expectedWasm: &wasmHash,
		},
		{
			name: "external-ref executable is skipped without panicking",
			val: instanceVal(xdr.ContractExecutable{
				Type: xdr.ContractExecutableTypeContractExecutableExternalRef,
				ExternalRef: &xdr.ContractExecutableExternalRef{
					ExecutableOwner: xdr.ScAddress{
						Type:       xdr.ScAddressTypeScAddressTypeContract,
						ContractId: &ownerID,
					},
					Tag: xdr.ScString("fleet-v1"),
				},
			}),
			expectSkip:     true,
			expectExternal: true,
		},
		{
			name: "external-ref executable with nil pointer is skipped without panicking",
			val: instanceVal(xdr.ContractExecutable{
				Type: xdr.ContractExecutableTypeContractExecutableExternalRef,
			}),
			expectSkip:     true,
			expectExternal: true,
		},
		{
			name: "wasm executable with nil hash is skipped",
			val: instanceVal(xdr.ContractExecutable{
				Type: xdr.ContractExecutableTypeContractExecutableWasm,
			}),
			expectSkip: true,
		},
		{
			name:       "instance Key with non-instance Val is skipped without panicking",
			val:        xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u32},
			expectSkip: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry := entryFor(tc.val)
			change := ingest.Change{
				Type: xdr.LedgerEntryTypeContractData,
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type:         xdr.LedgerEntryTypeContractData,
						ContractData: &entry,
					},
				},
			}

			before := testutil.ToFloat64(ingestionMetrics.ExternalRefContractsTotal)

			var result contractInstanceResult
			require.NotPanics(t, func() {
				result = svc.processContractInstanceChange(change, contractAddress, entry)
			})

			delta := testutil.ToFloat64(ingestionMetrics.ExternalRefContractsTotal) - before
			if tc.expectExternal {
				assert.Equal(t, float64(1), delta, "an external-ref executable must be counted, not silently skipped")
			} else {
				assert.Zero(t, delta)
			}

			assert.Equal(t, tc.expectSkip, result.Skip)
			if tc.expectedWasm != nil {
				require.NotNil(t, result.WasmHash)
				assert.Equal(t, *tc.expectedWasm, *result.WasmHash)
				require.NotNil(t, result.Contract)
				assert.Equal(t, contractAddress, result.Contract.ContractID)
			} else {
				assert.Nil(t, result.WasmHash)
			}
		})
	}
}
