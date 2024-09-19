package channels

import (
	"time"

	"github.com/stretchr/testify/mock"
)

type MockSleep struct {
	mock.Mock
}

func (m *MockSleep) Sleep(d time.Duration) {
	m.Called(d)
}
