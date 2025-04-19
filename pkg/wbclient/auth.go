package wbclient

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type SignerInterface interface {
	Sign(input []byte) ([]byte, error)
}

// RequestSigner is responsible for creating and adding signatures to HTTP requests.
type RequestSigner struct {
	Signer SignerInterface
}

// BuildSignatureHeader generates a signature for the given payload using the signer's keypair. The signature format is
// <timestamp>.<host>.<body>.
func (sc *RequestSigner) BuildSignatureHeader(hostname string, nowUnix int64, body []byte) (string, error) {
	// Ensure the hostname is parsed correctly
	if !strings.Contains(hostname, "://") {
		hostname = "https://" + hostname
	}
	host, err := url.ParseRequestURI(hostname)
	if err != nil {
		return "", fmt.Errorf("parsing hostname: %w", err)
	}

	signaturePayload := fmt.Sprintf("%d.%s.%s", nowUnix, host.Hostname(), string(body))
	signature, err := sc.Signer.Sign([]byte(signaturePayload))
	if err != nil {
		return "", fmt.Errorf("signing payload: %w", err)
	}

	encodedSig := base64.StdEncoding.EncodeToString(signature)

	signatureHeader := fmt.Sprintf("t=%d, s=%s", nowUnix, encodedSig)
	return signatureHeader, nil
}

// SignHTTPRequest adds the generated signature to the HTTP request's Signature header.
func (sc *RequestSigner) SignHTTPRequest(req *http.Request, nowUnix int64) error {
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return fmt.Errorf("reading request body: %w", err)
	}
	defer func() { // Reset the body so it can be read again
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}()

	signatureHeader, err := sc.BuildSignatureHeader(req.URL.Hostname(), nowUnix, bodyBytes)
	if err != nil {
		return fmt.Errorf("creating signature: %w", err)
	}
	req.Header.Set("Signature", signatureHeader)

	return nil
}
