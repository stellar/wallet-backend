package auth

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const DefaultMaxBodySizeBytes int64 = 102_400 // 100kb

var ErrUnauthorized = errors.New("not authorized")

// HTTPRequestSigner is responsible for signing HTTP requests using JWTs.
type HTTPRequestSigner interface {
	SignHTTPRequest(req *http.Request, timeout time.Duration) error
}

// HTTPRequestVerifier is responsible for verifying HTTP requests using JWTs.
type HTTPRequestVerifier interface {
	VerifyHTTPRequest(req *http.Request) error
}

// JWTHTTPSignerVerifier implements both signing and verifying of HTTP requests.
type JWTHTTPSignerVerifier struct {
	parser           JWTTokenParser
	generator        JWTTokenGenerator
	maxBodySizeBytes int64
}

func (s *JWTHTTPSignerVerifier) MaxBodySizeBytes() int64 {
	if s.maxBodySizeBytes == 0 {
		return DefaultMaxBodySizeBytes
	}
	return s.maxBodySizeBytes
}

// SignHTTPRequest signs an HTTP request with a JWT.
func (s *JWTHTTPSignerVerifier) SignHTTPRequest(req *http.Request, timeout time.Duration) error {
	// Read the request body
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return fmt.Errorf("reading request body: %w", err)
	}
	defer func() { // Reset the body so it can be read again
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}()

	// Generate the method and path
	methodAndPath := fmt.Sprintf("%s %s", req.Method, req.URL.Path)

	// Generate the token and sign the request
	jwtToken, err := s.generator.GenerateJWT(methodAndPath, bodyBytes, time.Now().Add(timeout))
	if err != nil {
		return fmt.Errorf("generating JWT token: %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", jwtToken))

	return nil
}

// VerifyHTTPRequest verifies the JWT in an HTTP request.
func (s *JWTHTTPSignerVerifier) VerifyHTTPRequest(req *http.Request) error {
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		return fmt.Errorf("missing Authorization header: %w", ErrUnauthorized)
	}

	// check if the Authorization header has two parts ['Bearer', token]
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return fmt.Errorf("the Authorization header is invalid, expected 'Bearer <token>': %w", ErrUnauthorized)
	}

	// Read the request body
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		if bodyBytes, err = io.ReadAll(io.LimitReader(req.Body, s.MaxBodySizeBytes())); err != nil {
			return fmt.Errorf("reading request body: %w", err)
		} else {
			defer func() { // Reset the body so it can be read again
				req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}()
		}
	}

	// Generate the method and path
	methodAndPath := fmt.Sprintf("%s %s", req.Method, req.URL.Path)

	// Parse the JWT
	tokenString := authHeader[len("Bearer "):] // Remove "Bearer " prefix
	_, _, err := s.parser.ParseJWT(tokenString, methodAndPath, bodyBytes)
	if err != nil {
		return fmt.Errorf("verifying JWT: %w: %w", err, ErrUnauthorized)
	}

	return nil
}

// NewHTTPRequestSigner creates a new HTTPRequestSigner with the given JWTTokenGenerator.
func NewHTTPRequestSigner(generator JWTTokenGenerator) HTTPRequestSigner {
	return &JWTHTTPSignerVerifier{
		generator: generator,
	}
}

// NewHTTPRequestVerifier creates a new HTTPRequestVerifier with the given JWTTokenParser.
func NewHTTPRequestVerifier(parser JWTTokenParser, maxBodySizeBytes int64) HTTPRequestVerifier {
	return &JWTHTTPSignerVerifier{
		parser:           parser,
		maxBodySizeBytes: maxBodySizeBytes,
	}
}
