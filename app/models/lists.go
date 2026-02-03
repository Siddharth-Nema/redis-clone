package models

import (
	"container/list"
	"sync"
)

type ListData struct {
	Mtx       sync.RWMutex
	Items     []string
	Semaphore *Semaphore
	Waiters   *list.List
}
