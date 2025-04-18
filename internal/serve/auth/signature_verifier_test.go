package auth

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignatureVerifierVerifySignature(t *testing.T) {
	host, err := url.ParseRequestURI("https://example.com")
	require.NoError(t, err)
	wrongHost, err := url.ParseRequestURI("https://wrong.example.com")
	require.NoError(t, err)

	rightBody := `{"value": "right value"}`
	wrongBody := `{"value": "wrong value"}`

	signingKey1 := keypair.MustRandom()
	signingKey2 := keypair.MustRandom()

	now := time.Now()
	nowUnix := now.Unix()
	expiredUnix := now.Add(-1000 * time.Hour).Unix()

	ctx := context.Background()
	signatureVerifier, err := NewStellarSignatureVerifier(host.String(), signingKey1.Address(), signingKey2.Address())
	require.NoError(t, err)

	testCases := []struct {
		name            string
		signer          *keypair.Full
		requestBody     string
		signedBody      string
		hostName        string
		timestamp       int64
		wantErrContains string
	}{
		{
			name:            "🔴expired_timestamp",
			signer:          signingKey1,
			requestBody:     rightBody,
			signedBody:      rightBody,
			hostName:        host.Hostname(),
			timestamp:       expiredUnix,
			wantErrContains: "timestamp expired by 1000h",
		},
		{
			name:            "🔴wrong_signer",
			signer:          keypair.MustRandom(),
			requestBody:     rightBody,
			signedBody:      rightBody,
			hostName:        host.Hostname(),
			timestamp:       nowUnix,
			wantErrContains: "unable to verify the signature for the given payload",
		},
		{
			name:            "🔴wrong_host",
			signer:          signingKey1,
			requestBody:     rightBody,
			signedBody:      rightBody,
			hostName:        wrongHost.Hostname(),
			timestamp:       nowUnix,
			wantErrContains: "unable to verify the signature for the given payload",
		},
		{
			name:            "🔴wrong_body",
			signer:          signingKey1,
			requestBody:     wrongBody,
			signedBody:      rightBody,
			hostName:        host.Hostname(),
			timestamp:       nowUnix,
			wantErrContains: "unable to verify the signature for the given payload",
		},
		{
			name:        "🟢successful/signerKey1/with_body",
			signer:      signingKey1,
			requestBody: rightBody,
			signedBody:  rightBody,
			hostName:    host.Hostname(),
			timestamp:   nowUnix,
		},
		{
			name:        "🟢successful/signerKey1/without_body",
			signer:      signingKey1,
			requestBody: "",
			signedBody:  "",
			hostName:    host.Hostname(),
			timestamp:   nowUnix,
		},
		{
			name:        "🟢successful/signerKey2/with_body",
			signer:      signingKey2,
			requestBody: rightBody,
			signedBody:  rightBody,
			hostName:    host.Hostname(),
			timestamp:   nowUnix,
		},
		{
			name:        "🟢successful/signerKey2/without_body",
			signer:      signingKey2,
			requestBody: "",
			signedBody:  "",
			hostName:    host.Hostname(),
			timestamp:   nowUnix,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sig := fmt.Sprintf("%d.%s.%s", tc.timestamp, tc.hostName, tc.signedBody)
			sig, err = tc.signer.SignBase64([]byte(sig))
			require.NoError(t, err)
			signatureHeaderContent := fmt.Sprintf("t=%d, s=%s", tc.timestamp, sig)

			err = signatureVerifier.VerifySignature(ctx, signatureHeaderContent, []byte(tc.requestBody))
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExtractTimestampedSignature(t *testing.T) {
	t.Run("invalid_header_content", func(t *testing.T) {
		ts, s, err := ExtractTimestampedSignature("")
		assert.EqualError(t, err, "malformed header: ")
		assert.Empty(t, ts)
		assert.Empty(t, s)

		ts, s, err = ExtractTimestampedSignature("a,b")
		var invalidTimestampFormatErr *InvalidTimestampFormatError
		assert.ErrorAs(t, err, &invalidTimestampFormatErr)
		assert.EqualError(t, err, "malformed timestamp: a")
		assert.Empty(t, ts)
		assert.Empty(t, s)

		ts, s, err = ExtractTimestampedSignature("t=abc,b")
		assert.EqualError(t, err, "malformed signature: [b]")
		assert.Empty(t, ts)
		assert.Empty(t, s)
	})

	t.Run("successfully_extracts_timestamp_and_signature", func(t *testing.T) {
		ts, s, err := ExtractTimestampedSignature("t=123,s=abc")
		assert.NoError(t, err)
		assert.Equal(t, "123", ts)
		assert.Equal(t, "abc", s)
	})
}

func TestVerifyGracePeriodSeconds(t *testing.T) {
	t.Run("invalid_timestamp", func(t *testing.T) {
		var invalidTimestampFormatErr *InvalidTimestampFormatError
		err := VerifyGracePeriodSeconds("", 2*time.Second)
		assert.ErrorAs(t, err, &invalidTimestampFormatErr)
		assert.EqualError(t, err, "signature format different than expected. expected unix seconds, got: ")

		err = VerifyGracePeriodSeconds("123", 2*time.Second)
		assert.ErrorAs(t, err, &invalidTimestampFormatErr)
		assert.EqualError(t, err, "signature format different than expected. expected unix seconds, got: 123")

		err = VerifyGracePeriodSeconds("12345678910", 2*time.Second)
		assert.ErrorAs(t, err, &invalidTimestampFormatErr)
		assert.EqualError(t, err, "signature format different than expected. expected unix seconds, got: 12345678910")
	})

	t.Run("successfully_verifies_grace_period", func(t *testing.T) {
		var expiredSignatureTimestampErr *ExpiredSignatureTimestampError
		now := time.Now().Add(-5 * time.Second)
		ts := now.Unix()
		err := VerifyGracePeriodSeconds(strconv.FormatInt(ts, 10), 2*time.Second)
		assert.ErrorAs(t, err, &expiredSignatureTimestampErr)
		assert.ErrorContains(t, err, "timestamp expired by 5")

		now = time.Now().Add(-1 * time.Second)
		ts = now.Unix()
		err = VerifyGracePeriodSeconds(strconv.FormatInt(ts, 10), 2*time.Second)
		assert.NoError(t, err)
	})
}
