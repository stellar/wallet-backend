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

	// Handle function return values (like Mockery does)
	if args.Get(0) != nil {
		if returnFunc, ok := args.Get(0).(func(string, string, io.Reader) *http.Response); ok {
			resp = returnFunc(url, contentType, body)
		} else {
			resp = args.Get(0).(*http.Response)
		}
	}

	if args.Error(1) != nil {
		err = args.Error(1)
	}

	return resp, err
}
