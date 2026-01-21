package main

import (
	"errors"
	"sync"
)

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
	ID     StreamID
	Values map[string]string
}

type Stream struct {
	mtx     *sync.RWMutex
	entries []StreamEntry
}

type StreamID struct {
	Time int64
	Seq  int64
}

var (
	ErrInvalidID       = errors.New("The ID specified in XADD is invalid")
	ErrIDTooSmall      = errors.New("The ID specified in XADD must be greater than 0-0")
	ErrIDNotIncreasing = errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
)
