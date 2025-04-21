package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go/keypair"
)

type SignatureVerifier interface {
	VerifySignature(ctx context.Context, signatureHeaderContent string, rawReqBody []byte) error
}

type InvalidTimestampFormatError struct {
	TimestampString     string
	timestampValueError bool
}

func (e InvalidTimestampFormatError) Error() string {
	if e.timestampValueError {
		return fmt.Sprintf("signature format different than expected. expected unix seconds, got: %s", e.TimestampString)
	}
	return fmt.Sprintf("malformed timestamp: %s", e.TimestampString)
}

type ExpiredSignatureTimestampError struct {
	ExpiredSignatureTimestamp time.Time
	CheckTime                 time.Time
}

func (e ExpiredSignatureTimestampError) Error() string {
	return fmt.Sprintf("timestamp expired by %s", e.CheckTime.Sub(e.ExpiredSignatureTimestamp).String())
}

type StellarSignatureVerifier struct {
	ServerHostname       string
	ClientAuthPublicKeys []keypair.FromAddress
}

var _ SignatureVerifier = (*StellarSignatureVerifier)(nil)

// VerifySignature verifies the Signature or X-Stellar-Signature content and checks if the signature is signed for a known caller.
func (sv *StellarSignatureVerifier) VerifySignature(ctx context.Context, signatureHeaderContent string, rawReqBody []byte) error {
	t, s, err := ExtractTimestampedSignature(signatureHeaderContent)
	if err != nil {
		return fmt.Errorf("unable to extract timestamped signature: %w", err)
	}

	// 2 seconds
	err = VerifyGracePeriodSeconds(t, 2*time.Second)
	if err != nil {
		return fmt.Errorf("signature timestamp has expired: %w", err)
	}

	signatureBytes, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("unable to decode signature value %s: %w", s, err)
	}

	payload := t + "." + sv.ServerHostname + "." + string(rawReqBody)

	if len(sv.ClientAuthPublicKeys) == 0 {
		return fmt.Errorf("no client auth public keys provided")
	}

	var sigVerifyErrors []error
	for _, kp := range sv.ClientAuthPublicKeys {
		if err := kp.Verify([]byte(payload), signatureBytes); err != nil {
			sigVerifyErrors = append(sigVerifyErrors, err)
			continue
		}
		return nil
	}

	return fmt.Errorf("unable to verify the signature for the given payload: %w", errors.Join(sigVerifyErrors...))
}

func ExtractTimestampedSignature(signatureHeaderContent string) (t string, s string, err error) {
	parts := strings.SplitN(signatureHeaderContent, ",", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("malformed header: %s", signatureHeaderContent)
	}

	tHeaderContent := parts[0]
	timestampParts := strings.SplitN(tHeaderContent, "=", 2)
	if len(timestampParts) != 2 || strings.TrimSpace(timestampParts[0]) != "t" {
		return "", "", &InvalidTimestampFormatError{TimestampString: tHeaderContent}
	}
	t = strings.TrimSpace(timestampParts[1])

	sHeaderContent := parts[1]
	signatureParts := strings.SplitN(sHeaderContent, "=", 2)
	if len(signatureParts) != 2 || strings.TrimSpace(signatureParts[0]) != "s" {
		return "", "", fmt.Errorf("malformed signature: %s", signatureParts)
	}
	s = strings.TrimSpace(signatureParts[1])

	return t, s, nil
}

func VerifyGracePeriodSeconds(timestampString string, gracePeriod time.Duration) error {
	// Note: from Nov 20th, 2286 this RegEx will fail because of an extra digit
	ok, err := regexp.MatchString(`^\d{10}$`, timestampString)
	if !ok {
		return &InvalidTimestampFormatError{TimestampString: timestampString, timestampValueError: true}
	}
	if err != nil {
		return fmt.Errorf("attempting to parse timestamp %q with regex: %w", timestampString, err)
	}

	timestampUnix, err := strconv.ParseInt(timestampString, 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse timestamp value %s: %w", timestampString, err)
	}

	return verifyGracePeriod(time.Unix(timestampUnix, 0), gracePeriod)
}

func verifyGracePeriod(timestamp time.Time, gracePeriod time.Duration) error {
	now := time.Now()
	if !timestamp.Add(gracePeriod).After(now) {
		return &ExpiredSignatureTimestampError{ExpiredSignatureTimestamp: timestamp, CheckTime: now}
	}

	return nil
}

func NewStellarSignatureVerifier(serverHostName string, clientAuthPublicKeys ...string) (*StellarSignatureVerifier, error) {
	if len(clientAuthPublicKeys) == 0 {
		return nil, fmt.Errorf("no client auth public keys provided")
	}

	kps := make([]keypair.FromAddress, len(clientAuthPublicKeys))
	for i, publicKey := range clientAuthPublicKeys {
		kp, err := keypair.ParseAddress(publicKey)
		if err != nil {
			return nil, fmt.Errorf("invalid client auth public key %q: %w", publicKey, err)
		}
		kps[i] = *kp
	}

	u, err := url.ParseRequestURI(serverHostName)
	if err != nil {
		return nil, fmt.Errorf("invalid server hostname: %w", err)
	}

	return &StellarSignatureVerifier{
		ServerHostname:       u.Hostname(),
		ClientAuthPublicKeys: kps,
	}, nil
}
