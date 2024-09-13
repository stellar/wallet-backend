package apptracker

type AppTracker interface {
	CaptureMessage(message string)    // Send a message to the app tracker
	CaptureException(exception error) // Send an exception to the app tracker
}
