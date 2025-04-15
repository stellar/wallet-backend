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
	signingKey := keypair.MustRandom()

	ctx := context.Background()
	signatureVerifier, err := NewStellarSignatureVerifier(host.String(), signingKey.Address())
	require.NoError(t, err)

	t.Run("returns_error_when_the_wallet_signing_key_is_not_the_singer", func(t *testing.T) {
		signer := keypair.MustRandom()
		now := time.Now()
		reqBody := `{"value": "new value"}`
		sig := fmt.Sprintf("%d.%s.%s", now.Unix(), host.Hostname(), reqBody)
		sig, err = signer.SignBase64([]byte(sig))
		require.NoError(t, err)
		signatureHeaderContent := fmt.Sprintf("t=%d, s=%s", now.Unix(), sig)

		err = signatureVerifier.VerifySignature(ctx, signatureHeaderContent, []byte(reqBody))
		assert.EqualError(t, err, ErrStellarSignatureNotVerified.Error())
	})

	t.Run("successfully_verifies_signature", func(t *testing.T) {
		now := time.Now()
		reqBody := `{"value": "new value"}`
		sig := fmt.Sprintf("%d.%s.%s", now.Unix(), host.Hostname(), reqBody)
		sig, err = signingKey.SignBase64([]byte(sig))
		require.NoError(t, err)
		signatureHeaderContent := fmt.Sprintf("t=%d, s=%s", now.Unix(), sig)

		err := signatureVerifier.VerifySignature(ctx, signatureHeaderContent, []byte(reqBody))
		assert.NoError(t, err)

		// When there's no request body
		now = time.Now()
		sig = fmt.Sprintf("%d.%s.%s", now.Unix(), host.Hostname(), "")
		sig, err = signingKey.SignBase64([]byte(sig))
		require.NoError(t, err)
		signatureHeaderContent = fmt.Sprintf("t=%d, s=%s", now.Unix(), sig)

		err = signatureVerifier.VerifySignature(ctx, signatureHeaderContent, []byte{})
		assert.NoError(t, err)
	})
}

func TestExtractTimestampedSignature(t *testing.T) {
	t.Run("invalid_header_content", func(t *testing.T) {
		ts, s, err := ExtractTimestampedSignature("")
		assert.EqualError(t, err, "malformed header: ")
		assert.Empty(t, ts)
		assert.Empty(t, s)

		ts, s, err = ExtractTimestampedSignature("a,b")
		var errTimestampFormat *ErrInvalidTimestampFormat
		assert.ErrorAs(t, err, &errTimestampFormat)
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
		var errTimestampFormat *ErrInvalidTimestampFormat
		err := VerifyGracePeriodSeconds("", 2*time.Second)
		assert.ErrorAs(t, err, &errTimestampFormat)
		assert.EqualError(t, err, "signature format different than expected. expected unix seconds, got: ")

		err = VerifyGracePeriodSeconds("123", 2*time.Second)
		assert.ErrorAs(t, err, &errTimestampFormat)
		assert.EqualError(t, err, "signature format different than expected. expected unix seconds, got: 123")

		err = VerifyGracePeriodSeconds("12345678910", 2*time.Second)
		assert.ErrorAs(t, err, &errTimestampFormat)
		assert.EqualError(t, err, "signature format different than expected. expected unix seconds, got: 12345678910")
	})

	t.Run("successfully_verifies_grace_period", func(t *testing.T) {
		var errExpiredSignatureTimestamp *ErrExpiredSignatureTimestamp
		now := time.Now().Add(-5 * time.Second)
		ts := now.Unix()
		err := VerifyGracePeriodSeconds(strconv.FormatInt(ts, 10), 2*time.Second)
		assert.ErrorAs(t, err, &errExpiredSignatureTimestamp)
		assert.ErrorContains(t, err, fmt.Sprintf("signature timestamp has expired. sig timestamp: %s, check time", now.Format(time.RFC3339)))

		now = time.Now().Add(-1 * time.Second)
		ts = now.Unix()
		err = VerifyGracePeriodSeconds(strconv.FormatInt(ts, 10), 2*time.Second)
		assert.NoError(t, err)
	})
}
