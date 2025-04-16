package sentry

import (
	"errors"
	"testing"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSentryTracker_CaptureMessage(t *testing.T) {
	mockSentry := setupMockSentry(t)
	mockSentry.
		On("Init", mock.Anything).Return(nil).Once().
		On("CaptureMessage", "Test message").Return((*sentry.EventID)(nil)).Once()

	tracker, err := NewSentryTracker("dsn", "test-env", 5)
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

	tracker, err := NewSentryTracker("dsn", "test-env", 5)
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
