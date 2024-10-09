package tss

import "github.com/stretchr/testify/mock"

type MockChannel struct {
	mock.Mock
}

func (m *MockChannel) Send(payload Payload) {
	m.Called(payload)
}

func (m *MockChannel) Receive(payload Payload) {
	m.Called(payload)
}

func (m *MockChannel) Stop() {
	m.Called()
}
