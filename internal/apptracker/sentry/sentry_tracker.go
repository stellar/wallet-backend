package sentry

import (
	"time"

	"github.com/getsentry/sentry-go"
)

// We need these variables to be able to mock sentry.CaptureMessage and sentry.CaptureException in tests since
// package level functions cannot be mocked
var (
	captureMessageFunc   = sentry.CaptureMessage
	captureExceptionFunc = sentry.CaptureException
	InitFunc             = sentry.Init
	FlushFunc            = sentry.Flush
	RecoverFunc          = sentry.Recover
)

type SentryTracker struct{}

func (s *SentryTracker) CaptureMessage(message string) {
	captureMessageFunc(message)
}

func (s *SentryTracker) CaptureException(exception error) {
	captureExceptionFunc(exception)
}

func NewSentryTracker(dsn string, env string, flushFreq int) (*SentryTracker, error) {
	if err := InitFunc(sentry.ClientOptions{
		Dsn:         dsn,
		Environment: env,
	}); err != nil {
		return nil, err
	}
	defer FlushFunc(time.Second * time.Duration(flushFreq))
	defer RecoverFunc()
	return &SentryTracker{}, nil

}
