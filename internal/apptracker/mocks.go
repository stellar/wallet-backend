package apptracker

import (
	"github.com/stretchr/testify/mock"
)

type MockAppTracker struct {
	mock.Mock
}

var _ AppTracker = (*MockAppTracker)(nil)

func (sv *MockAppTracker) CaptureMessage(message string) {
	sv.Called(message)
}

func (sv *MockAppTracker) CaptureException(exception error) {
	sv.Called(exception)
}
