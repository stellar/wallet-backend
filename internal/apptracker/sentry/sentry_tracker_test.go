package sentry

import (
	"errors"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

const testSentryDSN = "https://username@password.test.com/some-id"

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

func TestSentryTracker_CaptureMessage(t *testing.T) {
	mockSentry := setupMockSentry(t)
	mockSentry.
		On("Init", mock.Anything).Return(nil).Once().
		On("CaptureMessage", "Test message").Return((*sentry.EventID)(nil)).Once()

	tracker, err := NewSentryTracker(testSentryDSN, "test", 5)
	require.NoError(t, err)
	require.NotNil(t, tracker)

	tracker.CaptureMessage("Test message")
}

func TestSentryTracker_CaptureException(t *testing.T) {
	mockSentry := setupMockSentry(t)
	testError := errors.New("Test exception")
	mockSentry.
		On("Init", mock.Anything).Return(nil).Once().
		On("CaptureException", testError).Return((*sentry.EventID)(nil)).Once()

	tracker, err := NewSentryTracker(testSentryDSN, "test", 5)
	require.NoError(t, err)
	require.NotNil(t, tracker)

	tracker.CaptureException(testError)
}

func TestNewSentryTracker_Success(t *testing.T) {
	mockSentry := setupMockSentry(t)
	mockSentry.
		On("Init", mock.Anything).Return(nil).Once()

	tracker, err := NewSentryTracker("dsn", "test-env", 5)
	require.NoError(t, err)
	require.NotNil(t, tracker)
}

func TestNewSentryTracker_InitFailure(t *testing.T) {
	mockSentry := MockSentry{}
	InitFunc = mockSentry.Init
	t.Cleanup(func() {
		InitFunc = sentry.Init
		mockSentry.AssertExpectations(t)
	})

	initError := errors.New("init error")
	mockSentry.
		On("Init", mock.Anything).Return(initError).Once()

	tracker, err := NewSentryTracker("dsn", "test-env", 5)
	require.Error(t, err)
	require.ErrorContains(t, err, "unable to initialize sentry: init error")
	require.Nil(t, tracker)
}
