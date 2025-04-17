package wbclient

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type SignerInterface interface {
	Sign(input []byte) ([]byte, error)
}

// SignatureCreator is responsible for creating and adding signatures to HTTP requests.
type SignatureCreator struct {
	Signer SignerInterface
}

// CreateSignatureHeader generates a signature for the given payload using the signer's keypair. The signature format is
// <timestamp>.<host>.<body>.
func (sc *SignatureCreator) CreateSignatureHeader(hostname string, nowUnix int64, body []byte) (string, error) {
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

// AddSignatureToRequest adds the generated signature to the HTTP request's Signature header.
func (sc *SignatureCreator) AddSignatureToRequest(req *http.Request, body []byte) error {
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return fmt.Errorf("reading request body: %w", err)
	}
	defer func() { // Reset the body so it can be read again
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}()

	nowUnix := time.Now().Unix()
	signatureHeader, err := sc.CreateSignatureHeader(req.Host, nowUnix, bodyBytes)
	if err != nil {
		return fmt.Errorf("creating signature: %w", err)
	}
	req.Header.Set("Signature", signatureHeader)

	return nil
}
