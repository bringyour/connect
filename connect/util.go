


type Monitor struct {
	mutex sync.Mutex
	update chan struct{}
}

func wait(timeout) {

}

func WaitChannel() chan struct{} {
	return update
}

func notifyAll() {
	// close the update channel and create a new one
}

