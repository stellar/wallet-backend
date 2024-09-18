package utils

import (
	"io"
	"net/http"

	"github.com/stretchr/testify/mock"
)

type MockHTTPClient struct {
	mock.Mock
}

func (s *MockHTTPClient) Post(url, contentType string, body io.Reader) (resp *http.Response, err error) {
	args := s.Called(url, contentType, body)
	return args.Get(0).(*http.Response), args.Error(1)
}
