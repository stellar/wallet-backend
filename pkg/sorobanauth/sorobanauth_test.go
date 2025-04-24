package sorobanauth

import (
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_AuthSigner_AuthorizeEntry(t *testing.T) {
	testnetPassphrase := network.TestNetworkPassphrase
	pubnetPassphrase := network.PublicNetworkPassphrase
	nounce1 := int64(1)
	nounce2 := int64(2)
	validUntilLedgerSeq1 := uint32(11)
	validUntilLedgerSeq2 := uint32(22)
	// GAIG422GCQ5NPTYE34NYBELVKV543LMAQW3MTHEDZB7DPE673AOKLEXO
	signerKP1 := keypair.MustParseFull("SCZWYUF3CSYOYY2HPGRDKMAZ2YP3FXRXW77OLWO7KBOQ5BCSDURSHOJ5")
	// GBE5PW3RH2INLECD57ZRLZD7PF6C3JPY5DS4LRVMRUHYDI6Y4S6DB32D
	signerKP2 := keypair.MustParseFull("SAFX2XP6Q7PRPXGMXRD2CRHD33QFH2RBPUJTSGKJ6HH6ADDDOTWZKJRV")

	testCases := []struct {
		name                string
		networkPassphrase   string
		nounce              int64
		validUntilLedgerSeq uint32
		signerKP            *keypair.Full
		wantSignedXDR       string
		wantErrContains     string
	}{
		{
			name:                "🟢testnet,nounce1,validUntilLedgerSeq1,signerKP1",
			networkPassphrase:   testnetPassphrase,
			nounce:              nounce1,
			validUntilLedgerSeq: validUntilLedgerSeq1,
			signerKP:            signerKP1,
			wantSignedXDR:       "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAAAAAAAQAAAAsAAAAQAAAAAQAAAAEAAAARAAAAAQAAAAIAAAAPAAAACnB1YmxpY19rZXkAAAAAAA0AAAAgEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAPAAAACXNpZ25hdHVyZQAAAAAAAA0AAABAsw7GjhuY8jL9+OFqR6uNoX6+U51QDcUa/rFuTBk3eseUuLzsQlrEphfWwapENnPP8KfFouvqmqmcYX1ZENXkBQAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAA==",
		},
		{
			name:                "🟢pubnet,nounce1,validUntilLedgerSeq1,signerKP1",
			networkPassphrase:   pubnetPassphrase,
			nounce:              nounce1,
			validUntilLedgerSeq: validUntilLedgerSeq1,
			signerKP:            signerKP1,
			wantSignedXDR:       "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAAAAAAAQAAAAsAAAAQAAAAAQAAAAEAAAARAAAAAQAAAAIAAAAPAAAACnB1YmxpY19rZXkAAAAAAA0AAAAgEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAPAAAACXNpZ25hdHVyZQAAAAAAAA0AAABA8JUrfAIvRm20xA+TRw3P6iFNm2HgSIr2hc+0BfDLpPbrh34kz5+RiETLX8T5cLfbqfP02n5GCWXufPBafmYlAQAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAA==",
		},
		{
			name:                "🟢testnet,nounce2,validUntilLedgerSeq1,signerKP1",
			networkPassphrase:   testnetPassphrase,
			nounce:              nounce2,
			validUntilLedgerSeq: validUntilLedgerSeq1,
			signerKP:            signerKP1,
			wantSignedXDR:       "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAAAAAAAgAAAAsAAAAQAAAAAQAAAAEAAAARAAAAAQAAAAIAAAAPAAAACnB1YmxpY19rZXkAAAAAAA0AAAAgEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAPAAAACXNpZ25hdHVyZQAAAAAAAA0AAABAW4kd7/7zm3m9lhcmNhNGrj8GctyRYat8bWLc6H/S48AXwVWKi3mumhVslz5ADsmCFCout4Pqvu58unUZPMoxBwAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAA==",
		},
		{
			name:                "🟢testnet,nounce1,validUntilLedgerSeq2,signerKP1",
			networkPassphrase:   testnetPassphrase,
			nounce:              nounce1,
			validUntilLedgerSeq: validUntilLedgerSeq2,
			signerKP:            signerKP1,
			wantSignedXDR:       "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAAAAAAAQAAABYAAAAQAAAAAQAAAAEAAAARAAAAAQAAAAIAAAAPAAAACnB1YmxpY19rZXkAAAAAAA0AAAAgEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAPAAAACXNpZ25hdHVyZQAAAAAAAA0AAABAudJO7nIfoWBXHOmT3fxx6tJE1/3+aCk7JrnyuohUNLr7dA8KswnDJxMhVRSMzhH+uV80FO83dbdHDqQ15Gs/BgAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAA==",
		},
		{
			name:                "🟢testnet,nounce1,validUntilLedgerSeq2,signerKP2",
			networkPassphrase:   testnetPassphrase,
			nounce:              nounce1,
			validUntilLedgerSeq: validUntilLedgerSeq2,
			signerKP:            signerKP2,
			wantSignedXDR:       "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAAAAAAAQAAABYAAAAQAAAAAQAAAAEAAAARAAAAAQAAAAIAAAAPAAAACnB1YmxpY19rZXkAAAAAAA0AAAAgSdfbcT6Q1ZBD7/MV5H95fC2l+OjlxcasjQ+Bo9jkvDAAAAAPAAAACXNpZ25hdHVyZQAAAAAAAA0AAABAio/RAKzvmrwpu3DsGsYS77bRDZ+yKnSsGgMXEYu+kKITG0DHW5P1YcFTF/myH0duISROoxqmlPGx2rffjBtcCAAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAA==",
		},
	}

	authEntryXDR := "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKVxxKYXs/ivgQAAAAAAAAABAAAAAAAAAAHXkotywnA8z+r365/0701QSlWouXn8m0UOoshCtNHOYQAAAAh0cmFuc2ZlcgAAAAMAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAABIAAAAAAAAAABBua0YUOtfPBN8bgJF1VXvNrYCFtsmcg8h+N5Pf2BylAAAACgAAAAAAAAAAAAAAAAX14QAAAAAA"
	authEntry := xdr.SorobanAuthorizationEntry{}
	err := xdr.SafeUnmarshalBase64(authEntryXDR, &authEntry)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			authSigner := AuthSigner{NetworkPassphrase: tc.networkPassphrase}
			authorizedEntry, err := authSigner.AuthorizeEntry(authEntry, tc.nounce, tc.validUntilLedgerSeq, tc.signerKP)
			require.NoError(t, err)

			signedEntryXDR, err := xdr.MarshalBase64(authorizedEntry)
			require.NoError(t, err)
			assert.Equal(t, tc.wantSignedXDR, signedEntryXDR)
		})
	}
}
