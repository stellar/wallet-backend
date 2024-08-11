package apptracker

type AppTracker interface {
	CaptureMessage(message string)
	CaptureException(exception error)
}
