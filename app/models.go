package main

import "sync"

type waiter struct {
	ch chan result
}

type result struct {
	val string
	ok  bool
}

type listState struct {
	items []string
}

// Stream
type StreamEntry struct {
	ID     string
	Values map[string]string
}

type Stream struct {
	mtx     *sync.RWMutex
	entries []StreamEntry
}
