package sentry

import (
	"errors"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/assert"
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

func TestSentryTracker_CaptureMessage(t *testing.T) {
	mockSentry := MockSentry{}
	captureMessageFunc = mockSentry.CaptureMessage
	defer func() { captureMessageFunc = sentry.CaptureMessage }()
	mockSentry.On("CaptureMessage", "Test message").Return((*sentry.EventID)(nil))
	tracker, _ := NewSentryTracker("sentrydsn", "test", 5)
	tracker.CaptureMessage("Test message")

	mockSentry.AssertCalled(t, "CaptureMessage", "Test message")
}

func TestSentryTracker_CaptureException(t *testing.T) {
	mockSentry := MockSentry{}
	captureExceptionFunc = mockSentry.CaptureException
	defer func() { captureExceptionFunc = sentry.CaptureException }() // Reset after the test
	testError := errors.New("Test exception")
	mockSentry.On("CaptureException", testError).Return((*sentry.EventID)(nil))
	tracker, _ := NewSentryTracker("sentrydsn", "test", 5)
	tracker.CaptureException(testError)

	mockSentry.AssertCalled(t, "CaptureException", testError)
}

func TestNewSentryTracker_Success(t *testing.T) {
	mockSentry := MockSentry{}

	InitFunc = mockSentry.Init
	FlushFunc = mockSentry.Flush
	RecoverFunc = mockSentry.Recover

	defer func() {
		InitFunc = sentry.Init
		FlushFunc = sentry.Flush
		RecoverFunc = sentry.Recover
	}()

	mockSentry.On("Init", mock.Anything).Return(nil)
	mockSentry.On("Flush", time.Second*5).Return(true)
	mockSentry.On("Recover").Return((*sentry.EventID)(nil))

	tracker, err := NewSentryTracker("dsn", "test-env", 5)

	assert.NoError(t, err)
	assert.NotNil(t, tracker)

	mockSentry.AssertCalled(t, "Init", mock.Anything)
	mockSentry.AssertCalled(t, "Flush", time.Second*5)
	mockSentry.AssertCalled(t, "Recover")
}

func TestNewSentryTracker_InitFailure(t *testing.T) {
	mockSentry := MockSentry{}
	InitFunc = mockSentry.Init
	defer func() {
		InitFunc = sentry.Init
	}()
	initError := errors.New("init error")
	mockSentry.On("Init", mock.Anything).Return(initError)
	tracker, err := NewSentryTracker("dsn", "test-env", 5)
	assert.Error(t, err)
	assert.Equal(t, initError, err)
	assert.Nil(t, tracker)

	mockSentry.AssertCalled(t, "Init", mock.Anything)
}
