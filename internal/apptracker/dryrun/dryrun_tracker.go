package dryrun

import "fmt"

type DryRunTracker struct{}

func (d *DryRunTracker) CaptureMessage(message string) {
	fmt.Println(message)
}

func (s *DryRunTracker) CaptureException(exception error) {
	fmt.Println(exception)
}
