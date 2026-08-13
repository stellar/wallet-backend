package processors

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Addresses used by the sponsorship fixtures. They only need to be distinct, valid
// strkeys; the signer constants are ordered so that sponsoredSigner sorts before
// otherSigner, which pins the emission order asserted below.
const (
	sponsoredAccount = "GC4XF7RE3R4P77GY5XNGICM56IOKUURWAAANPXHFC7G5H6FCNQVVH3OH"
	firstSponsor     = "GAQHWQYBBW272OOXNQMMLCA5WY2XAZPODGB7Q3S5OKKIXVESKO55ZQ7C"
	secondSponsor    = "GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY"
	sponsoredSigner  = "GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON"
	otherSigner      = "GCCOBXW2XQNUSL467IEILE6MMCNRR66SSVL4YQADUNYYNUVREF3FIV2Z"
)

// signerSponsor pairs a signer with the account sponsoring it. An empty sponsor
// means the signer is unsponsored, which core encodes as a nil descriptor at the
// signer's position.
type signerSponsor struct {
	signer  string
	sponsor string
}

// sponsorshipAccountEntry builds a bare account ledger entry: no extensions on
// either the ledger entry or the account, so neither sponsorship reader sees a
// sponsor.
func sponsorshipAccountEntry(accountID string) *xdr.LedgerEntry {
	return &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.AccountEntry{
			AccountId: xdr.MustAddress(accountID),
		},
	}}
}

// signerSponsorshipEntry builds an account entry carrying the given signers and
// their sponsors. SponsorPerSigner zips Signers against SignerSponsoringIDs by
// position, so the two slices are written in lockstep.
func signerSponsorshipEntry(accountID string, pairs ...signerSponsor) *xdr.LedgerEntry {
	entry := sponsorshipAccountEntry(accountID)
	if len(pairs) == 0 {
		return entry
	}

	acct := entry.Data.Account
	sponsoringIDs := make([]xdr.SponsorshipDescriptor, 0, len(pairs))
	for _, pair := range pairs {
		acct.Signers = append(acct.Signers, xdr.Signer{
			Key:    xdr.MustSigner(pair.signer),
			Weight: 1,
		})
		if pair.sponsor == "" {
			sponsoringIDs = append(sponsoringIDs, nil)
			continue
		}
		sponsor := xdr.MustAddress(pair.sponsor)
		sponsoringIDs = append(sponsoringIDs, &sponsor)
	}

	acct.Ext = xdr.AccountEntryExt{
		V: 1,
		V1: &xdr.AccountEntryExtensionV1{
			Ext: xdr.AccountEntryExtensionV1Ext{
				V:  2,
				V2: &xdr.AccountEntryExtensionV2{SignerSponsoringIDs: sponsoringIDs},
			},
		},
	}
	return entry
}

// withSponsoringID stamps the sponsor of the ledger entry itself. This is a
// different extension chain from the per-signer sponsors: SponsoringID() reads
// LedgerEntry.Ext.V1, while the signer sponsors live under AccountEntry.Ext.V1.Ext.V2.
func withSponsoringID(entry *xdr.LedgerEntry, sponsor string) *xdr.LedgerEntry {
	sponsorID := xdr.MustAddress(sponsor)
	entry.Ext = xdr.LedgerEntryExt{
		V:  1,
		V1: &xdr.LedgerEntryExtensionV1{SponsoringId: &sponsorID},
	}
	return entry
}

// newSponsorshipEffectsWrapper returns an empty wrapper bound to a real operation,
// which the effect appenders need for the operation ID they stamp on every effect.
func newSponsorshipEffectsWrapper(t *testing.T) *effectsWrapper {
	t.Helper()
	transaction := createTx(setTrustlineFlagsOp(), nil, nil, false)
	op, found := transaction.GetOperation(0)
	require.True(t, found)
	return &effectsWrapper{
		effects: []EffectOutput{},
		operation: &TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    &transaction,
			LedgerSequence: 12345,
		},
	}
}

// TestEffects_SignerSponsorshipPredicate covers accountHasSponsoredSigner, the
// early-out guard that spares addSignerSponsorshipEffects from building a
// SponsorPerSigner map for the overwhelming majority of account changes, which
// sponsor no signer at all. The guard must never report false for an entry that
// actually carries a sponsor.
func TestEffects_SignerSponsorshipPredicate(t *testing.T) {
	// An account extended to V2 but with every sponsoring slot empty: two signers,
	// neither sponsored.
	unsponsored := signerSponsorshipEntry(sponsoredAccount,
		signerSponsor{signer: sponsoredSigner},
		signerSponsor{signer: otherSigner},
	)

	// V1 extension present, V2 absent: the reader must stop at the missing V2
	// rather than dereference it.
	v1Only := sponsorshipAccountEntry(sponsoredAccount)
	v1Only.Data.Account.Ext = xdr.AccountEntryExt{V: 1, V1: &xdr.AccountEntryExtensionV1{}}

	// A non-account entry reaches the predicate through the type switch in Effects,
	// so the nil Account pointer must be handled rather than dereferenced.
	nonAccount := &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
		Type: xdr.LedgerEntryTypeTrustline,
		TrustLine: &xdr.TrustLineEntry{
			AccountId: xdr.MustAddress(sponsoredAccount),
			Asset:     usdcAsset.ToTrustLineAsset(),
		},
	}}

	testCases := []struct {
		name  string
		entry *xdr.LedgerEntry
		want  bool
	}{
		{
			name:  "a missing entry side sponsors nothing",
			entry: nil,
			want:  false,
		},
		{
			name:  "an entry that is not an account sponsors nothing",
			entry: nonAccount,
			want:  false,
		},
		{
			name:  "an account with no extensions sponsors nothing",
			entry: sponsorshipAccountEntry(sponsoredAccount),
			want:  false,
		},
		{
			name:  "an account extended to V1 only sponsors nothing",
			entry: v1Only,
			want:  false,
		},
		{
			name:  "an account whose sponsoring IDs are all nil sponsors nothing",
			entry: unsponsored,
			want:  false,
		},
		{
			name: "a single non-nil sponsoring ID is enough",
			entry: signerSponsorshipEntry(sponsoredAccount,
				signerSponsor{signer: sponsoredSigner},
				signerSponsor{signer: otherSigner, sponsor: firstSponsor},
			),
			want: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, accountHasSponsoredSigner(tc.entry))
		})
	}
}

// TestEffects_AddSignerSponsorshipEffects covers the signer-sponsorship transitions
// derived from an account change: which effect each pre/post pairing yields, whose
// address it is attributed to, and the cases that must yield nothing at all.
func TestEffects_AddSignerSponsorshipEffects(t *testing.T) {
	t.Run("a change to a non-account entry yields no effects", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		wrapper.addSignerSponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeTrustline,
			Pre:  signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner, sponsor: firstSponsor}),
			Post: signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner}),
		})
		assert.Empty(t, wrapper.effects)
	})

	t.Run("an account sponsoring no signer on either side yields no effects", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		wrapper.addSignerSponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner}),
			Post: signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner}, signerSponsor{signer: otherSigner}),
		})
		assert.Empty(t, wrapper.effects)
	})

	t.Run("an account created with an unsponsored signer yields no effects", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		wrapper.addSignerSponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Post: signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner}),
		})
		assert.Empty(t, wrapper.effects)
	})

	t.Run("a signer gaining a sponsor is a creation attributed to the account", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		wrapper.addSignerSponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner}),
			Post: signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner, sponsor: firstSponsor}),
		})

		require.Len(t, wrapper.effects, 1)
		effect := wrapper.effects[0]
		assert.Equal(t, sponsoredAccount, effect.Address)
		assert.Equal(t, int32(EffectSignerSponsorshipCreated), effect.Type)
		assert.Equal(t, "signer_sponsorship_created", effect.TypeString)
		assert.Equal(t, toid.New(12345, 1, 1).ToInt64(), effect.OperationID)
		assert.Equal(t, map[string]interface{}{
			"sponsor": firstSponsor,
			"signer":  sponsoredSigner,
		}, effect.Details)
	})

	t.Run("a signer losing its sponsor is a removal attributed to the pre-image account", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		wrapper.addSignerSponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner, sponsor: firstSponsor}),
			Post: signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner}),
		})

		require.Len(t, wrapper.effects, 1)
		effect := wrapper.effects[0]
		assert.Equal(t, sponsoredAccount, effect.Address)
		assert.Equal(t, int32(EffectSignerSponsorshipRemoved), effect.Type)
		assert.Equal(t, "signer_sponsorship_removed", effect.TypeString)
		assert.Equal(t, map[string]interface{}{
			"former_sponsor": firstSponsor,
			"signer":         sponsoredSigner,
		}, effect.Details)
	})

	t.Run("removing the whole account removes its signer sponsorship", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		wrapper.addSignerSponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner, sponsor: firstSponsor}),
		})

		require.Len(t, wrapper.effects, 1)
		assert.Equal(t, int32(EffectSignerSponsorshipRemoved), wrapper.effects[0].Type)
		assert.Equal(t, map[string]interface{}{
			"former_sponsor": firstSponsor,
			"signer":         sponsoredSigner,
		}, wrapper.effects[0].Details)
	})

	t.Run("a sponsor carried unchanged across the change yields no effects", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		wrapper.addSignerSponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner, sponsor: firstSponsor}),
			Post: signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner, sponsor: firstSponsor}),
		})
		assert.Empty(t, wrapper.effects)
	})

	t.Run("a signer changing sponsors is an update carrying both sponsors", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		wrapper.addSignerSponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner, sponsor: firstSponsor}),
			Post: signerSponsorshipEntry(sponsoredAccount, signerSponsor{signer: sponsoredSigner, sponsor: secondSponsor}),
		})

		require.Len(t, wrapper.effects, 1)
		assert.Equal(t, int32(EffectSignerSponsorshipUpdated), wrapper.effects[0].Type)
		assert.Equal(t, map[string]interface{}{
			"former_sponsor": firstSponsor,
			"new_sponsor":    secondSponsor,
			"signer":         sponsoredSigner,
		}, wrapper.effects[0].Details)
	})

	// The guard only reads the sponsoring IDs, while SponsorPerSigner zips them
	// against the signer list. An entry with a sponsoring ID but no matching signer
	// therefore passes the guard and still yields nothing, which is the safe
	// direction: the guard may over-admit, never over-reject.
	t.Run("a sponsoring ID with no matching signer yields no effects", func(t *testing.T) {
		post := sponsorshipAccountEntry(sponsoredAccount)
		sponsorID := xdr.MustAddress(firstSponsor)
		post.Data.Account.Ext = xdr.AccountEntryExt{
			V: 1,
			V1: &xdr.AccountEntryExtensionV1{
				Ext: xdr.AccountEntryExtensionV1Ext{
					V:  2,
					V2: &xdr.AccountEntryExtensionV2{SignerSponsoringIDs: []xdr.SponsorshipDescriptor{&sponsorID}},
				},
			},
		}
		require.True(t, accountHasSponsoredSigner(post))

		wrapper := newSponsorshipEffectsWrapper(t)
		wrapper.addSignerSponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  sponsorshipAccountEntry(sponsoredAccount),
			Post: post,
		})
		assert.Empty(t, wrapper.effects)
	})

	// Core delivers signers in an unordered map, so the effects are sorted by signer
	// address to keep re-ingests of the same operation byte-identical.
	t.Run("effects for several signers are emitted in signer address order", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		wrapper.addSignerSponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre: signerSponsorshipEntry(sponsoredAccount,
				signerSponsor{signer: otherSigner, sponsor: firstSponsor},
				signerSponsor{signer: sponsoredSigner},
			),
			Post: signerSponsorshipEntry(sponsoredAccount,
				signerSponsor{signer: otherSigner},
				signerSponsor{signer: sponsoredSigner, sponsor: secondSponsor},
			),
		})

		require.Len(t, wrapper.effects, 2)
		assert.Equal(t, sponsoredSigner, wrapper.effects[0].Details["signer"])
		assert.Equal(t, int32(EffectSignerSponsorshipCreated), wrapper.effects[0].Type)
		assert.Equal(t, otherSigner, wrapper.effects[1].Details["signer"])
		assert.Equal(t, int32(EffectSignerSponsorshipRemoved), wrapper.effects[1].Type)
	})
}

// TestEffects_AddLedgerEntrySponsorshipEffects covers the sponsorship of the ledger
// entry itself, which is read from a different extension than the per-signer
// sponsors and is attributed to the account owning the entry.
func TestEffects_AddLedgerEntrySponsorshipEffects(t *testing.T) {
	t.Run("an account gaining a sponsor is a creation", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  sponsorshipAccountEntry(sponsoredAccount),
			Post: withSponsoringID(sponsorshipAccountEntry(sponsoredAccount), firstSponsor),
		})
		require.NoError(t, err)

		require.Len(t, wrapper.effects, 1)
		effect := wrapper.effects[0]
		assert.Equal(t, sponsoredAccount, effect.Address)
		assert.Equal(t, int32(EffectAccountSponsorshipCreated), effect.Type)
		assert.Equal(t, "account_sponsorship_created", effect.TypeString)
		assert.Equal(t, toid.New(12345, 1, 1).ToInt64(), effect.OperationID)
		assert.Equal(t, map[string]interface{}{"sponsor": firstSponsor}, effect.Details)
	})

	t.Run("a sponsored account created from nothing is a creation", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Post: withSponsoringID(sponsorshipAccountEntry(sponsoredAccount), firstSponsor),
		})
		require.NoError(t, err)

		require.Len(t, wrapper.effects, 1)
		assert.Equal(t, int32(EffectAccountSponsorshipCreated), wrapper.effects[0].Type)
		assert.Equal(t, map[string]interface{}{"sponsor": firstSponsor}, wrapper.effects[0].Details)
	})

	t.Run("an account losing its sponsor is a removal", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  withSponsoringID(sponsorshipAccountEntry(sponsoredAccount), firstSponsor),
			Post: sponsorshipAccountEntry(sponsoredAccount),
		})
		require.NoError(t, err)

		require.Len(t, wrapper.effects, 1)
		effect := wrapper.effects[0]
		assert.Equal(t, sponsoredAccount, effect.Address)
		assert.Equal(t, int32(EffectAccountSponsorshipRemoved), effect.Type)
		assert.Equal(t, "account_sponsorship_removed", effect.TypeString)
		assert.Equal(t, map[string]interface{}{"former_sponsor": firstSponsor}, effect.Details)
	})

	// A removed entry has no post-image, so the effect must be built from the
	// pre-image alone.
	t.Run("removing a sponsored account is a removal read from the pre-image", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  withSponsoringID(sponsorshipAccountEntry(sponsoredAccount), firstSponsor),
		})
		require.NoError(t, err)

		require.Len(t, wrapper.effects, 1)
		assert.Equal(t, sponsoredAccount, wrapper.effects[0].Address)
		assert.Equal(t, int32(EffectAccountSponsorshipRemoved), wrapper.effects[0].Type)
		assert.Equal(t, map[string]interface{}{"former_sponsor": firstSponsor}, wrapper.effects[0].Details)
	})

	t.Run("an account changing sponsors is an update carrying both sponsors", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  withSponsoringID(sponsorshipAccountEntry(sponsoredAccount), firstSponsor),
			Post: withSponsoringID(sponsorshipAccountEntry(sponsoredAccount), secondSponsor),
		})
		require.NoError(t, err)

		require.Len(t, wrapper.effects, 1)
		assert.Equal(t, int32(EffectAccountSponsorshipUpdated), wrapper.effects[0].Type)
		assert.Equal(t, map[string]interface{}{
			"former_sponsor": firstSponsor,
			"new_sponsor":    secondSponsor,
		}, wrapper.effects[0].Details)
	})

	t.Run("an account keeping the same sponsor yields no effect", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  withSponsoringID(sponsorshipAccountEntry(sponsoredAccount), firstSponsor),
			Post: withSponsoringID(sponsorshipAccountEntry(sponsoredAccount), firstSponsor),
		})
		require.NoError(t, err)
		assert.Empty(t, wrapper.effects)
	})

	t.Run("an unsponsored account yields no effect and no error", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  sponsorshipAccountEntry(sponsoredAccount),
			Post: sponsorshipAccountEntry(sponsoredAccount),
		})
		require.NoError(t, err)
		assert.Empty(t, wrapper.effects)
	})

	t.Run("a sponsored trustline names the asset and belongs to the trustor", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		trustline := &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: xdr.MustAddress(sponsoredAccount),
				Asset:     usdcAsset.ToTrustLineAsset(),
			},
		}}
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeTrustline,
			Post: withSponsoringID(trustline, firstSponsor),
		})
		require.NoError(t, err)

		require.Len(t, wrapper.effects, 1)
		effect := wrapper.effects[0]
		assert.Equal(t, sponsoredAccount, effect.Address)
		assert.Equal(t, int32(EffectTrustlineSponsorshipCreated), effect.Type)
		assert.Equal(t, map[string]interface{}{
			"sponsor": firstSponsor,
			"asset":   "USDC:" + usdcIssuer,
		}, effect.Details)
	})

	// A pool-share trustline has no issuer to render, so it is described by the pool
	// it holds shares in instead of by a canonical asset string.
	t.Run("a sponsored pool share trustline names the liquidity pool", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		poolShare := &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTrustline,
			TrustLine: &xdr.TrustLineEntry{
				AccountId: xdr.MustAddress(sponsoredAccount),
				Asset: xdr.TrustLineAsset{
					Type:            xdr.AssetTypeAssetTypePoolShare,
					LiquidityPoolId: &lpBtcEthID,
				},
			},
		}}
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeTrustline,
			Pre:  withSponsoringID(poolShare, firstSponsor),
		})
		require.NoError(t, err)

		require.Len(t, wrapper.effects, 1)
		effect := wrapper.effects[0]
		assert.Equal(t, sponsoredAccount, effect.Address)
		assert.Equal(t, int32(EffectTrustlineSponsorshipRemoved), effect.Type)
		assert.Equal(t, map[string]interface{}{
			"former_sponsor":    firstSponsor,
			"asset_type":        "liquidity_pool",
			"liquidity_pool_id": PoolIDToString(lpBtcEthID),
		}, effect.Details)
	})

	// Data entries and claimable balances are attributed to the operation source
	// rather than to an account read off the entry, so the effect can land on a
	// muxed address.
	t.Run("a sponsored data entry is attributed to the operation source", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		dataEntry := &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeData,
			Data: &xdr.DataEntry{
				AccountId: xdr.MustAddress(sponsoredAccount),
				DataName:  xdr.String64("config_a"),
				DataValue: xdr.DataValue("v1"),
			},
		}}
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeData,
			Post: withSponsoringID(dataEntry, firstSponsor),
		})
		require.NoError(t, err)

		require.Len(t, wrapper.effects, 1)
		effect := wrapper.effects[0]
		source := wrapper.operation.SourceAccount()
		sourceID := source.ToAccountId()
		assert.Equal(t, sourceID.Address(), effect.Address)
		assert.NotEqual(t, sponsoredAccount, effect.Address)
		assert.Equal(t, int32(EffectDataSponsorshipCreated), effect.Type)
		assert.Equal(t, map[string]interface{}{
			"sponsor":   firstSponsor,
			"data_name": "config_a",
		}, effect.Details)
	})

	t.Run("a sponsored claimable balance carries its hex balance id", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		cbEntry := cbLedgerEntry(someBalanceID, xlmAsset, 100)
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeClaimableBalance,
			Post: withSponsoringID(&cbEntry, firstSponsor),
		})
		require.NoError(t, err)

		require.Len(t, wrapper.effects, 1)
		effect := wrapper.effects[0]
		source := wrapper.operation.SourceAccount()
		sourceID := source.ToAccountId()
		assert.Equal(t, sourceID.Address(), effect.Address)
		assert.Equal(t, int32(EffectClaimableBalanceSponsorshipCreated), effect.Type)
		// The discriminant of the V0 balance-id union followed by its 32-byte hash.
		assert.Equal(t, map[string]interface{}{
			"sponsor":    firstSponsor,
			"balance_id": "000000000102030405000000000000000000000000000000000000000000000000000000",
		}, effect.Details)
	})

	// Liquidity pools cannot be sponsored, so they are absent from the effect table
	// and must be dropped before the entry-type switch, which would reject them.
	t.Run("an entry type that cannot be sponsored yields no effect and no error", func(t *testing.T) {
		wrapper := newSponsorshipEffectsWrapper(t)
		err := wrapper.addLedgerEntrySponsorshipEffects(ingest.Change{
			Type: xdr.LedgerEntryTypeLiquidityPool,
			Post: &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
				Type:          xdr.LedgerEntryTypeLiquidityPool,
				LiquidityPool: &xdr.LiquidityPoolEntry{LiquidityPoolId: lpBtcEthID},
			}},
		})
		require.NoError(t, err)
		assert.Empty(t, wrapper.effects)
	})
}
