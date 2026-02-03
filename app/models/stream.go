package models

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
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

var MaxStreamID = StreamEntryID{
	Time: math.MaxInt64,
	Seq:  math.MaxInt64,
}

var MinStreamID = StreamEntryID{
	Time: 0,
	Seq:  0,
}

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

func (stream *Stream) ValidateEntryID(entryID string) error {
	newID, err := stream.GenerateStreamIDFromString(entryID)
	if err != nil {
		return err
	}

	stream.Mtx.RLock()
	if len(stream.Entries) == 0 {
		stream.Mtx.RUnlock()
		return nil
	}
	lastID := stream.Entries[len(stream.Entries)-1].ID
	stream.Mtx.RUnlock()

	if !newID.GreaterThan(lastID) {
		return ErrIDNotIncreasing
	}

	return nil
}

func (stream *Stream) GenerateStreamIDFromString(s string) (StreamEntryID, error) {
	if s == "*" {
		return AutoGenerateCompleteID(), nil
	}

	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return StreamEntryID{}, ErrInvalidID
	}

	t, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return StreamEntryID{}, ErrInvalidID
	}
	var seq int64

	if parts[1] == "*" {
		var lastID StreamEntryID
		if len(stream.Entries) == 0 {
			if t == 0 {
				seq = 1
			} else {
				seq = 0
			}
		} else {
			lastID = stream.Entries[len(stream.Entries)-1].ID
			if lastID.Time == t {
				seq = lastID.Seq + 1
			} else {
				seq = 0
			}
		}
	} else {
		seq, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return StreamEntryID{}, ErrInvalidID
		}
	}

	if t < 0 || seq < 0 || (t == 0 && seq == 0) {
		return StreamEntryID{}, ErrIDTooSmall
	}

	return StreamEntryID{Time: t, Seq: seq}, nil
}

func (stream *Stream) RemoveWaiterFromStream(waiter chan struct{}) {
	stream.Mtx.Lock()
	defer stream.Mtx.Unlock()
	for i, w := range stream.Waiters {
		if w == waiter {
			stream.Waiters = append(
				stream.Waiters[:i],
				stream.Waiters[i+1:]...,
			)
			return
		}
	}
}

// HELPERS
func AutoGenerateCompleteID() StreamEntryID {
	time := time.Now().UnixMilli()
	var newID StreamEntryID
	newID.Time = time
	newID.Seq = 0

	return newID
}

func (streamID *StreamEntryID) StreamIDToString() string {
	return strconv.FormatInt(streamID.Time, 10) + "-" + strconv.FormatInt(streamID.Seq, 10)
}

func ParseStreamIDFromString(s string) (StreamEntryID, error) {

	if s == "-" {
		return MinStreamID, nil
	}

	if s == "+" {
		return MaxStreamID, nil
	}

	time, seq, ok := strings.Cut(s, "-")
	var err error

	if !ok {
		seq = "0"
	}

	var id StreamEntryID
	id.Time, err = strconv.ParseInt(time, 10, 64)
	if err != nil {
		return id, err
	}

	id.Seq, err = strconv.ParseInt(seq, 10, 64)
	if err != nil {
		return id, err
	}

	return id, nil

}
