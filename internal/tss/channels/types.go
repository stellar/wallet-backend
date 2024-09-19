package channels

import "time"

var sleep = time.Sleep

type WorkerPool interface {
	Submit(task func())
	StopAndWait()
}
