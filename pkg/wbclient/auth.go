package wbclient

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return fmt.Errorf("reading request body: %w", err)
	}
	defer func() { // Reset the body so it can be read again
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}()

	u, err := url.ParseRequestURI(req.URL.Scheme + "://" + req.Host)
	if err != nil {
		return fmt.Errorf("parsing hostname: %w", err)
	}

	jwtToken, err := sc.GenerateJWT(u.Hostname(), bodyBytes, time.Now().Add(timeout))
	if err != nil {
		return fmt.Errorf("generating JWT token: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", jwtToken))

	return nil
}
