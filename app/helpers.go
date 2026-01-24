package main

import (
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

func parseStreamIDFromString(s string) (models.StreamID, error) {
	time, seq, ok := strings.Cut(s, "-")
	var err error

	if !ok {
		seq = "0"
	}

	var id models.StreamID
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

func autoGenerateCompleteID() models.StreamID {
	time := time.Now().UnixMilli()
	var newID models.StreamID
	newID.Time = time
	newID.Seq = 0

	return newID

}
func generateStreamIDFromString(s string, stream *models.Stream) (models.StreamID, error) {
	if s == "*" {
		return autoGenerateCompleteID(), nil
	}

	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return models.StreamID{}, models.ErrInvalidID
	}

	t, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return models.StreamID{}, models.ErrInvalidID
	}
	var seq int64

	if parts[1] == "*" {
		var lastID models.StreamID
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
			return models.StreamID{}, models.ErrInvalidID
		}
	}

	if t < 0 || seq < 0 || (t == 0 && seq == 0) {
		return models.StreamID{}, models.ErrIDTooSmall
	}

	return models.StreamID{Time: t, Seq: seq}, nil
}

func streamIDToString(streamID models.StreamID) string {
	return strconv.FormatInt(streamID.Time, 10) + "-" + strconv.FormatInt(streamID.Seq, 10)
}

func validateEntryID(entryID string, stream *models.Stream) error {
	newID, err := generateStreamIDFromString(entryID, stream)
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
		return models.ErrIDNotIncreasing
	}

	return nil
}
