package models

import (
	"errors"
	"strconv"
	"sync"
)

// Stream
type StreamEntry struct {
	ID     StreamEntryID
	Values []string
}

type Stream struct {
	Mtx     *sync.RWMutex
	Entries []StreamEntry
	Waiters []chan struct{}
}

type StreamEntryID struct {
	Time int64
	Seq  int64
}

type ReadStream struct {
	StreamID     string
	StartEntryID StreamEntryID
}

type StreamOutput struct {
	StreamID      string
	StreamEntries []StreamEntry
}

var (
	ErrInvalidID       = errors.New("The ID specified in XADD is invalid")
	ErrIDTooSmall      = errors.New("The ID specified in XADD must be greater than 0-0")
	ErrIDNotIncreasing = errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
)

func (a StreamEntryID) GreaterThan(b StreamEntryID) bool {
	return a.Time > b.Time || (a.Time == b.Time && a.Seq > b.Seq)
}

func (id StreamEntryID) String() string {
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

func StreamOutputToReply(outputs []StreamOutput) [][]interface{} {
	result := make([][]interface{}, 0, len(outputs))

	for _, out := range outputs {
		result = append(result, []interface{}{
			out.StreamID,
			StreamEntriesToReply(out.StreamEntries),
		})
	}

	return result
}

func (stream *Stream) HasEntriesAfter(requestedID StreamEntryID) bool {
	stream.Mtx.RLock()
	defer stream.Mtx.RUnlock()

	if stream.Entries[len(stream.Entries)-1].ID.GreaterThan(requestedID) {
		return true
	} else {
		return false
	}

}
