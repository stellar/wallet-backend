package channels

type WorkerPool interface {
	Submit(task func())
	StopAndWait()
}
