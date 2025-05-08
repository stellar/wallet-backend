package wbclient

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

type SignerInterface interface {
	Sign(input []byte) ([]byte, error)
}

// RequestSigner is responsible for creating and adding signatures to HTTP requests.
type RequestSigner struct {
	auth.JWTTokenGenerator
}

// SignHTTPRequest adds the generated JWT token to the HTTP request's Authorization header.
func (sc *RequestSigner) SignHTTPRequest(req *http.Request, timeout time.Duration) error {
	// Ensure the hostname is parsed correctly
	hostname := req.URL.Hostname()
	if !strings.Contains(hostname, "://") {
		hostname = "https://" + hostname
	}
	host, err := url.ParseRequestURI(hostname)
	if err != nil {
		return fmt.Errorf("parsing hostname: %w", err)
	}

	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return fmt.Errorf("reading request body: %w", err)
	}
	defer func() { // Reset the body so it can be read again
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}()

	jwtToken, err := sc.GenerateJWT(host.Hostname(), bodyBytes, time.Now().Add(timeout))
	if err != nil {
		return fmt.Errorf("generating JWT token: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", jwtToken))

	return nil
}
