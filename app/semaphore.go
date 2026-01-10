package main

import "time"

type Semaphore struct {
	ch chan struct{}
}

func NewSemaphore() *Semaphore {
	return &Semaphore{ch: make(chan struct{}, 1000)}
}

func (s *Semaphore) Acquire() {
	<-s.ch
}

func (s *Semaphore) Release() {
	s.ch <- struct{}{}
}

func (s *Semaphore) ReleaseN(n int) {
	for i := 0; i < n; i++ {
		s.ch <- struct{}{}
	}
}

func (s *Semaphore) AcquireTimeout(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-s.ch:
		return true
	case <-timer.C:
		return false
	}
}

func (s *Semaphore) releaseTimeout(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case s.ch <- struct{}{}:
		return true
	case <-timer.C:
		return false
	}
}
