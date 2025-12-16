package sorobanauth

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
)

func Test_AuthSigner_AuthorizeEntry(t *testing.T) {
	testnetPassphrase := network.TestNetworkPassphrase
	pubnetPassphrase := network.PublicNetworkPassphrase
	nonce1 := int64(1)
	nonce2 := int64(2)
	validUntilLedgerSeq1 := uint32(11)
	validUntilLedgerSeq2 := uint32(22)
	// GAIG422GCQ5NPTYE34NYBELVKV543LMAQW3MTHEDZB7DPE673AOKLEXO
	signerKP1 := keypair.MustParseFull("SCZWYUF3CSYOYY2HPGRDKMAZ2YP3FXRXW77OLWO7KBOQ5BCSDURSHOJ5")
	// GBE5PW3RH2INLECD57ZRLZD7PF6C3JPY5DS4LRVMRUHYDI6Y4S6DB32D
	signerKP2 := keypair.MustParseFull("SAFX2XP6Q7PRPXGMXRD2CRHD33QFH2RBPUJTSGKJ6HH6ADDDOTWZKJRV")

	const unsignedAuthEntryXDR = "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKVxxKYXs/ivgQAAAAAAAAABAAAAAAAAAAHXkotywnA8z+r365/0701QSlWouXn8m0UOoshCtNHOYQAAAAh0cmFuc2ZlcgAAAAMAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAABIAAAAAAAAAABBua0YUOtfPBN8bgJF1VXvNrYCFtsmcg8h+N5Pf2BylAAAACgAAAAAAAAAAAAAAAAX14QAAAAAA"
	authEntry := xdr.SorobanAuthorizationEntry{}
	err := xdr.SafeUnmarshalBase64(unsignedAuthEntryXDR, &authEntry)
	require.NoError(t, err)

	testCases := []struct {
		name                string
		networkPassphrase   string
		nonce               int64
		validUntilLedgerSeq uint32
		signerKP            *keypair.Full
		authEntry           xdr.SorobanAuthorizationEntry
		wantSignedXDR       string
		wantErrContains     string
	}{
		{
			name:                "🔴unsupported_credentials_type",
			networkPassphrase:   testnetPassphrase,
			nonce:               nonce1,
			validUntilLedgerSeq: validUntilLedgerSeq1,
			signerKP:            signerKP1,
			authEntry: xdr.SorobanAuthorizationEntry{
				Credentials: xdr.SorobanCredentials{
					Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount,
				},
			},
			wantErrContains: (&UnsupportedCredentialsTypeError{CredentialsType: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount}).Error(),
		},
		{
			name:                "🟢testnet,nonce1,validUntilLedgerSeq1,signerKP1",
			networkPassphrase:   testnetPassphrase,
			nonce:               nonce1,
			validUntilLedgerSeq: validUntilLedgerSeq1,
			signerKP:            signerKP1,
			authEntry:           authEntry,
			wantSignedXDR:       "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAAAAAAAQAAAAsAAAAQAAAAAQAAAAEAAAARAAAAAQAAAAIAAAAPAAAACnB1YmxpY19rZXkAAAAAAA0AAAAgEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAPAAAACXNpZ25hdHVyZQAAAAAAAA0AAABAsw7GjhuY8jL9+OFqR6uNoX6+U51QDcUa/rFuTBk3eseUuLzsQlrEphfWwapENnPP8KfFouvqmqmcYX1ZENXkBQAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAA==",
		},
		{
			name:                "🟢pubnet,nonce1,validUntilLedgerSeq1,signerKP1",
			networkPassphrase:   pubnetPassphrase,
			nonce:               nonce1,
			validUntilLedgerSeq: validUntilLedgerSeq1,
			signerKP:            signerKP1,
			authEntry:           authEntry,
			wantSignedXDR:       "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAAAAAAAQAAAAsAAAAQAAAAAQAAAAEAAAARAAAAAQAAAAIAAAAPAAAACnB1YmxpY19rZXkAAAAAAA0AAAAgEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAPAAAACXNpZ25hdHVyZQAAAAAAAA0AAABA8JUrfAIvRm20xA+TRw3P6iFNm2HgSIr2hc+0BfDLpPbrh34kz5+RiETLX8T5cLfbqfP02n5GCWXufPBafmYlAQAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAA==",
		},
		{
			name:                "🟢testnet,nonce2,validUntilLedgerSeq1,signerKP1",
			networkPassphrase:   testnetPassphrase,
			nonce:               nonce2,
			validUntilLedgerSeq: validUntilLedgerSeq1,
			signerKP:            signerKP1,
			authEntry:           authEntry,
			wantSignedXDR:       "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAAAAAAAgAAAAsAAAAQAAAAAQAAAAEAAAARAAAAAQAAAAIAAAAPAAAACnB1YmxpY19rZXkAAAAAAA0AAAAgEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAPAAAACXNpZ25hdHVyZQAAAAAAAA0AAABAW4kd7/7zm3m9lhcmNhNGrj8GctyRYat8bWLc6H/S48AXwVWKi3mumhVslz5ADsmCFCout4Pqvu58unUZPMoxBwAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAA==",
		},
		{
			name:                "🟢testnet,nonce1,validUntilLedgerSeq2,signerKP1",
			networkPassphrase:   testnetPassphrase,
			nonce:               nonce1,
			validUntilLedgerSeq: validUntilLedgerSeq2,
			signerKP:            signerKP1,
			authEntry:           authEntry,
			wantSignedXDR:       "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAAAAAAAQAAABYAAAAQAAAAAQAAAAEAAAARAAAAAQAAAAIAAAAPAAAACnB1YmxpY19rZXkAAAAAAA0AAAAgEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAPAAAACXNpZ25hdHVyZQAAAAAAAA0AAABAudJO7nIfoWBXHOmT3fxx6tJE1/3+aCk7JrnyuohUNLr7dA8KswnDJxMhVRSMzhH+uV80FO83dbdHDqQ15Gs/BgAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAA==",
		},
		{
			name:                "🟢testnet,nonce1,validUntilLedgerSeq2,signerKP2",
			networkPassphrase:   testnetPassphrase,
			nonce:               nonce1,
			validUntilLedgerSeq: validUntilLedgerSeq2,
			signerKP:            signerKP2,
			authEntry:           authEntry,
			wantSignedXDR:       "AAAAAQAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAAAAAAAAQAAABYAAAAQAAAAAQAAAAEAAAARAAAAAQAAAAIAAAAPAAAACnB1YmxpY19rZXkAAAAAAA0AAAAgSdfbcT6Q1ZBD7/MV5H95fC2l+OjlxcasjQ+Bo9jkvDAAAAAPAAAACXNpZ25hdHVyZQAAAAAAAA0AAABAio/RAKzvmrwpu3DsGsYS77bRDZ+yKnSsGgMXEYu+kKITG0DHW5P1YcFTF/myH0duISROoxqmlPGx2rffjBtcCAAAAAAAAAAB15KLcsJwPM/q9+uf9O9NUEpVqLl5/JtFDqLIQrTRzmEAAAAIdHJhbnNmZXIAAAADAAAAEgAAAAAAAAAAEG5rRhQ6188E3xuAkXVVe82tgIW2yZyDyH43k9/YHKUAAAASAAAAAAAAAAAQbmtGFDrXzwTfG4CRdVV7za2AhbbJnIPIfjeT39gcpQAAAAoAAAAAAAAAAAAAAAAF9eEAAAAAAA==",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			authSigner := AuthSigner{NetworkPassphrase: tc.networkPassphrase}
			authorizedEntry, err := authSigner.AuthorizeEntry(tc.authEntry, tc.nonce, tc.validUntilLedgerSeq, tc.signerKP)
			if tc.wantErrContains == "" {
				require.NoError(t, err)
				var signedEntryXDR string
				signedEntryXDR, err = xdr.MarshalBase64(authorizedEntry)
				require.NoError(t, err)
				assert.Equal(t, tc.wantSignedXDR, signedEntryXDR)
			} else {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Equal(t, tc.authEntry, authorizedEntry)
			}
		})
	}
}

func Test_CheckForForbiddenSigners(t *testing.T) {
	forbiddenSigner1 := "GAIG422GCQ5NPTYE34NYBELVKV543LMAQW3MTHEDZB7DPE673AOKLEXO"
	forbiddenSigner1AccountID, err := xdr.AddressToAccountId(forbiddenSigner1)
	require.NoError(t, err)
	forbiddenSigner2 := "GBE5PW3RH2INLECD57ZRLZD7PF6C3JPY5DS4LRVMRUHYDI6Y4S6DB32D"
	forbiddenSigners := []string{forbiddenSigner1, forbiddenSigner2}
	allowedSigner1 := "GCYNTH5HDQRNIQ3BSSYPWFO5AHH5ERVZ32C37QRXT6TXK3OJFFOIVXDE"
	allowedSigner2 := "GDSL6NQIMQ76EOJZ7Y7MUQJYKL4UTFR4TSCSOQEKUB2F7M4KRAW3NGFH"
	allowedSigner2AccountID, err := xdr.AddressToAccountId(allowedSigner2)
	require.NoError(t, err)

	testCases := []struct {
		name                      string
		simulationResponseResults []entities.RPCSimulateHostFunctionResult
		opSourceAccount           string
		forbiddenSigners          []string
		wantErrContains           string
	}{
		{
			name: "🔴CredentialsSourceAccount/opSourceAccount_cannot_be_empty",
			simulationResponseResults: []entities.RPCSimulateHostFunctionResult{
				{
					Auth: []xdr.SorobanAuthorizationEntry{
						{
							Credentials: xdr.SorobanCredentials{
								Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount,
							},
						},
					},
				},
			},
			opSourceAccount:  "",
			forbiddenSigners: forbiddenSigners,
			wantErrContains:  "handling SorobanCredentialsTypeSorobanCredentialsSourceAccount: " + ErrForbiddenSigner.Error(),
		},
		{
			name: "🟢CredentialsSourceAccount/opSourceAccount_allowedSigner1",
			simulationResponseResults: []entities.RPCSimulateHostFunctionResult{
				{
					Auth: []xdr.SorobanAuthorizationEntry{
						{
							Credentials: xdr.SorobanCredentials{
								Type: xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount,
							},
						},
					},
				},
			},
			opSourceAccount:  allowedSigner1,
			forbiddenSigners: forbiddenSigners,
		},
		{
			name: "🔴CredentialsAddress/empty_address",
			simulationResponseResults: []entities.RPCSimulateHostFunctionResult{
				{
					Auth: []xdr.SorobanAuthorizationEntry{
						{
							Credentials: xdr.SorobanCredentials{
								Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
							},
						},
					},
				},
			},
			opSourceAccount:  allowedSigner1,
			forbiddenSigners: forbiddenSigners,
			wantErrContains:  "unable to get auth entry signer because credential address is nil",
		},
		{
			name: "🔴CredentialsAddress/opSourceAccount_forbiddenSigner1",
			simulationResponseResults: []entities.RPCSimulateHostFunctionResult{
				{
					Auth: []xdr.SorobanAuthorizationEntry{
						{
							Credentials: xdr.SorobanCredentials{
								Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
								Address: &xdr.SorobanAddressCredentials{
									Address: xdr.ScAddress{
										Type:      xdr.ScAddressTypeScAddressTypeAccount,
										AccountId: &forbiddenSigner1AccountID,
									},
								},
							},
						},
					},
				},
			},
			opSourceAccount:  forbiddenSigner1,
			forbiddenSigners: forbiddenSigners,
			wantErrContains:  "handling SorobanCredentialsTypeSorobanCredentialsAddress: " + ErrForbiddenSigner.Error(),
		},
		{
			name: "🟢CredentialsAddress/opSourceAccount_allowedSigner2",
			simulationResponseResults: []entities.RPCSimulateHostFunctionResult{
				{
					Auth: []xdr.SorobanAuthorizationEntry{
						{
							Credentials: xdr.SorobanCredentials{
								Type: xdr.SorobanCredentialsTypeSorobanCredentialsAddress,
								Address: &xdr.SorobanAddressCredentials{
									Address: xdr.ScAddress{
										Type:      xdr.ScAddressTypeScAddressTypeAccount,
										AccountId: &allowedSigner2AccountID,
									},
								},
							},
						},
					},
				},
			},
			opSourceAccount:  forbiddenSigner1,
			forbiddenSigners: forbiddenSigners,
		},
		{
			name: "🔴CredentialsUnsupported",
			simulationResponseResults: []entities.RPCSimulateHostFunctionResult{
				{
					Auth: []xdr.SorobanAuthorizationEntry{
						{
							Credentials: xdr.SorobanCredentials{
								Type: 3,
							},
						},
					},
				},
			},
			wantErrContains: "unsupported auth entry type 3",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckForForbiddenSigners(tc.simulationResponseResults, tc.opSourceAccount, tc.forbiddenSigners...)
			if tc.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
