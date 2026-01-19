package main

import (
	"errors"
	"strconv"
	"strings"
)

func validateEntryID(entryID string, streamPtr *Stream) error {
	entryIDParts := strings.SplitN(entryID, "-", 2)
	if len(entryIDParts) != 2 {
		return errors.New("The ID specified in XADD is invalid")
	}

	entryTime, err := strconv.ParseInt(entryIDParts[0], 10, 64)
	if err != nil {
		return errors.New("The ID specified in XADD is invalid")
	}

	entrySeq, err := strconv.ParseInt(entryIDParts[1], 10, 64)
	if err != nil {
		return errors.New("The ID specified in XADD is invalid")
	}

	if entryTime < 0 || entrySeq < 0 || (entryTime == 0 && entrySeq == 0) {
		return errors.New("The ID specified in XADD must be greater than 0-0")
	}

	streamPtr.mtx.RLock()
	if len(streamPtr.entries) == 0 {
		streamPtr.mtx.RUnlock()
		return nil
	}
	lastEntryID := streamPtr.entries[len(streamPtr.entries)-1].ID
	streamPtr.mtx.RUnlock()

	lastEntryIDParts := strings.SplitN(lastEntryID, "-", 2)
	if len(lastEntryIDParts) != 2 {
		return errors.New("The ID specified in XADD is invalid")
	}

	lastTime, err := strconv.ParseInt(lastEntryIDParts[0], 10, 64)
	if err != nil {
		return errors.New("The ID specified in XADD is invalid")
	}

	lastSeq, err := strconv.ParseInt(lastEntryIDParts[1], 10, 64)
	if err != nil {
		return errors.New("The ID specified in XADD is invalid")
	}

	if entryTime > lastTime || (entryTime == lastTime && entrySeq > lastSeq) {
		return nil
	}
	return errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
}
