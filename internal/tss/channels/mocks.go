package channels

import "github.com/stretchr/testify/mock"

type WorkerPoolMock struct {
	mock.Mock
}

func (m *WorkerPoolMock) Submit(task func()) {
	m.Called(task)
}

func (m *WorkerPoolMock) Stop() {
	m.Called()
}
