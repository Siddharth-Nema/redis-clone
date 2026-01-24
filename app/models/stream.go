package models

import (
	"errors"
	"strconv"
	"sync"
)

// Stream
type StreamEntry struct {
	ID     StreamID
	Values []string
}

type Stream struct {
	Mtx     *sync.RWMutex
	Entries []StreamEntry
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

func (a StreamID) GreaterThan(b StreamID) bool {
	return a.Time > b.Time || (a.Time == b.Time && a.Seq > b.Seq)
}

func (id StreamID) String() string {
	return strconv.FormatInt(id.Time, 10) + "-" +
		strconv.FormatInt(id.Seq, 10)
}

func StreamEntriesToReply(entries []StreamEntry) [][]interface{} {
	reply := make([][]interface{}, 0, len(entries))

	for _, entry := range entries {
		item := []interface{}{
			entry.ID.String(),
			entry.Values,
		}
		reply = append(reply, item)
	}

	return reply
}
