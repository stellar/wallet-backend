package sentry

import (
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/mock"
)

// MockSentry is a mock struct to capture function calls
type MockSentry struct {
	mock.Mock
}

func (m *MockSentry) CaptureMessage(message string) *sentry.EventID {
	args := m.Called(message)
	return args.Get(0).(*sentry.EventID)
}

func (m *MockSentry) CaptureException(exception error) *sentry.EventID {
	args := m.Called(exception)
	return args.Get(0).(*sentry.EventID)
}

func (m *MockSentry) Init(options sentry.ClientOptions) error {
	args := m.Called(options)
	return args.Error(0)
}

func (m *MockSentry) Flush(timeout time.Duration) bool {
	m.Called(timeout)
	return true
}

func (m *MockSentry) Recover() *sentry.EventID {
	args := m.Called()
	return args.Get(0).(*sentry.EventID)
}

func setupMockSentry(t *testing.T) *MockSentry {
	t.Helper()

	mockSentry := &MockSentry{}
	mockSentry.
		On("Flush", mock.Anything).Return(true).Once().
		On("Recover").Return((*sentry.EventID)(nil)).Once()

	// Save the original functions
	originalInitFunc := InitFunc
	originalFlushFunc := FlushFunc
	originalRecoverFunc := RecoverFunc
	originalCaptureMessageFunc := captureMessageFunc
	originalCaptureExceptionFunc := captureExceptionFunc

	// Set the mock functions
	InitFunc = mockSentry.Init
	FlushFunc = mockSentry.Flush
	RecoverFunc = mockSentry.Recover
	captureMessageFunc = mockSentry.CaptureMessage
	captureExceptionFunc = mockSentry.CaptureException

	// Restore the original functions after the test
	t.Cleanup(func() {
		InitFunc = originalInitFunc
		FlushFunc = originalFlushFunc
		RecoverFunc = originalRecoverFunc
		captureMessageFunc = originalCaptureMessageFunc
		captureExceptionFunc = originalCaptureExceptionFunc
		mockSentry.AssertExpectations(t)
	})

	return mockSentry
}
