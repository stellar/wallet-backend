package sentry

import (
	"time"

	"github.com/getsentry/sentry-go"
)

type SentryTracker struct {
	FlushFreq int64
}

func (s *SentryTracker) CaptureMessage(message string) {
	sentry.CaptureMessage(message)
}

func (s *SentryTracker) CaptureException(exception error) {
	sentry.CaptureException(exception)
}

func (s *SentryTracker) InitTracker(dsn string, stellarEnv string) error {
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:         dsn,
		Environment: stellarEnv,
	}); err != nil {
		return err
	}
	defer sentry.Flush(time.Second * time.Duration(s.FlushFreq))
	defer sentry.Recover()
	return nil
}
