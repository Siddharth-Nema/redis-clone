package main

import (
	"strconv"
	"strings"
	"time"
)

func autoGenerateCompleteID() StreamID {
	time := time.Now().UnixMilli()
	var newID StreamID
	newID.Time = time
	newID.Seq = 0

	return newID
}

func generateStreamIDFromString(s string, stream *Stream) (StreamID, error) {

	if s == "*" {
		return autoGenerateCompleteID(), nil
	}

	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return StreamID{}, ErrInvalidID
	}

	t, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, ErrInvalidID
	}
	var seq int64

	if parts[1] == "*" {
		var lastID StreamID
		if len(stream.entries) == 0 {
			if t == 0 {
				seq = 1
			} else {
				seq = 0
			}
		} else {
			lastID = stream.entries[len(stream.entries)-1].ID
			if lastID.Time == t {
				seq = lastID.Seq + 1
			} else {
				seq = 0
			}
		}
	} else {
		seq, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return StreamID{}, ErrInvalidID
		}
	}

	if t < 0 || seq < 0 || (t == 0 && seq == 0) {
		return StreamID{}, ErrIDTooSmall
	}

	return StreamID{Time: t, Seq: seq}, nil
}

func streamIDToString(streamID StreamID) string {
	return strconv.FormatInt(streamID.Time, 10) + "-" + strconv.FormatInt(streamID.Seq, 10)
}

func (a StreamID) GreaterThan(b StreamID) bool {
	return a.Time > b.Time || (a.Time == b.Time && a.Seq > b.Seq)
}

func validateEntryID(entryID string, stream *Stream) error {
	newID, err := generateStreamIDFromString(entryID, stream)
	if err != nil {
		return err
	}

	stream.mtx.RLock()
	defer stream.mtx.RUnlock()

	if len(stream.entries) == 0 {
		return nil
	}

	lastID := stream.entries[len(stream.entries)-1].ID

	if !newID.GreaterThan(lastID) {
		return ErrIDNotIncreasing
	}

	return nil
}
