package util

type Semaphore struct {
	permits  int
	syncChan chan int
}

func NewSemaphore(permits int) (s *Semaphore) {
	log.Info("NewSemaphore %d", permits)
	return &Semaphore{permits: permits, syncChan: make(chan int, permits)}
}

// acquire n resources
func (s *Semaphore) P(n int) {
	for i := 0; i < n; i++ {
		s.syncChan <- 1
	}
}

// release n resources
func (s *Semaphore) V(n int) {
	for i := 0; i < n; i++ {
		<-s.syncChan
	}
}

func (s *Semaphore) Acquire() {
	// log.Printf("Semaphore.Acquire")
	s.P(1)
}

func (s *Semaphore) Release() {
	// log.Printf("Semaphore.Release")
	s.V(1)
}
