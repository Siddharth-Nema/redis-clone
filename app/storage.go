package main

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

func getType(key string) string {
	keysMtx.RLock()
	defer keysMtx.RUnlock()
	keyType, exists := keys[key]

	if exists {
		return keyType
	}

	return "none"
}

func setType(key string, keyType string) {
	keysMtx.Lock()
	defer keysMtx.Unlock()
	keys[key] = keyType
}

func set(key string, val string) {
	storeMtx.Lock()
	defer storeMtx.Unlock()
	store[key] = val
	setType(key, "string")
}

func get(key string) (string, bool) {
	storeMtx.Lock()
	defer storeMtx.Unlock()

	val, ok := store[key]
	return val, ok
}

func setExpiry(key string, timestamp time.Time) {
	expiryMtx.Lock()
	defer expiryMtx.Unlock()

	expiry[key] = timestamp
}

func getExpiry(key string) time.Time {
	expiryMtx.Lock()
	defer expiryMtx.Unlock()

	val, ok := expiry[key]
	if ok {
		return val
	} else {
		return time.Time{}
	}
}

func getOrCreateList(key string) *ListData {
	listMtx.Lock()
	defer listMtx.Unlock()

	if _, exists := listStore[key]; !exists {
		listStore[key] = &ListData{
			items:     []string{},
			semaphore: NewSemaphore(),
			waiters:   list.New(),
		}
		setType(key, "list")
	}
	return listStore[key]
}

func deleteKey(key string) error {
	_, exists := store[key]
	if !exists {
		return fmt.Errorf("Key does not exist")
	}

	storeMtx.Lock()
	defer storeMtx.Unlock()

	delete(store, key)

	expiryMtx.Lock()
	defer expiryMtx.Unlock()

	delete(expiry, key)

	listMtx.Lock()
	defer listMtx.Unlock()

	delete(listStore, key)

	return nil
}

func pushToList(key string, vals []string) int {
	ld := getOrCreateList(key)
	ld.mu.Lock()
	defer ld.mu.Unlock()

	newLen := len(vals) + len(ld.items)

	i := 0
	for i < len(vals) && ld.waiters.Len() > 0 {
		e := ld.waiters.Front()
		w := e.Value.(*models.Waiter)
		ld.waiters.Remove(e)

		w.Ch <- models.Result{Val: vals[i], Ok: true}
		i++
	}

	if i < len(vals) {
		ld.items = append(ld.items, vals[i:]...)
		ld.semaphore.ReleaseN(len(vals) - i)
	}

	return newLen
}

func prependToList(key string, vals []string) int {
	ld := getOrCreateList(key)
	ld.mu.Lock()
	defer ld.mu.Unlock()

	i := len(vals) - 1
	for i >= 0 && ld.waiters.Len() > 0 {
		e := ld.waiters.Front()
		w := e.Value.(*models.Waiter)
		ld.waiters.Remove(e)

		w.Ch <- models.Result{Val: vals[i], Ok: true}
		i--
	}

	if i >= 0 {
		remaining := vals[:i+1]
		ld.items = append(vals, ld.items...)

		ld.semaphore.ReleaseN(len(remaining))
	}

	return len(ld.items)

}

func getItemsFromList(key string, start int, end int) []string {
	ld := getOrCreateList(key)
	ld.mu.RLock()
	defer ld.mu.RUnlock()

	reqList := ld.items
	size := len(reqList)
	if start < 0 {
		start += size
	}
	if end < 0 {
		end += size
	}

	start = max(start, 0)
	end = min(end, size-1)

	if start > end || start > len(reqList) {
		return []string{}
	}

	return reqList[start : end+1]
}

func blockingLPop(key string, timeoutSecs float64) (string, bool) {
	ld := getOrCreateList(key)
	ld.mu.Lock()

	if len(ld.items) > 0 {
		val := ld.items[0]
		ld.items = ld.items[1:]
		ld.mu.Unlock()
		return val, true
	}

	w := &models.Waiter{Ch: make(chan models.Result, 1)}
	elem := ld.waiters.PushBack(w)
	ld.mu.Unlock()

	if timeoutSecs <= 0 {
		res := <-w.Ch
		return res.Val, res.Ok
	} else {
		timeout := time.Duration(timeoutSecs * float64(time.Second))
		select {
		case res := <-w.Ch:
			return res.Val, res.Ok
		case <-time.After(timeout):
			ld.mu.Lock()
			ld.waiters.Remove(elem)
			ld.mu.Unlock()
			return "", false
		}
	}
}

func getLength(key string) int {
	ld := getOrCreateList(key)
	ld.mu.RLock()
	defer ld.mu.RUnlock()

	return len(ld.items)
}

func popFromLeftofArray(key string, count int) ([]string, bool) {
	ld := getOrCreateList(key)
	ld.mu.Lock()
	defer ld.mu.Unlock()

	if len(ld.items) >= count {
		val := ld.items[0:count]
		ld.items = ld.items[count:]
		return val, true
	} else {
		return []string{}, false
	}
}

func createStream(key string) {
	streamStoreMtx.Lock()
	defer streamStoreMtx.Unlock()

	streamStore[key] = &models.Stream{
		Mtx:     &sync.RWMutex{},
		Entries: []models.StreamEntry{},
	}

	keysMtx.Lock()
	keys[key] = "stream"
	keysMtx.Unlock()
}

func addToStream(key string, entryID string, values []string) (string, error) {
	streamStoreMtx.RLock()
	streamPtr, exists := streamStore[key]
	streamStoreMtx.RUnlock()

	if !exists {
		createStream(key)
		streamStoreMtx.RLock()
		streamPtr = streamStore[key]
		streamStoreMtx.RUnlock()
	}

	err := validateEntryID(entryID, streamPtr)

	if err != nil {
		return "", err
	}

	streamPtr.Mtx.Lock()
	defer streamPtr.Mtx.Unlock()

	var newEntry models.StreamEntry
	newEntry.ID, err = generateStreamIDFromString(entryID, streamPtr)
	if err != nil {
		return "", err
	}

	newEntry.Values = values

	streamPtr.Entries = append(streamPtr.Entries, newEntry)

	for _, ch := range streamPtr.Waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	return streamIDToString(newEntry.ID), nil
}

func getStreamEntries(key string, startingEntryID models.StreamEntryID, endingEntryID models.StreamEntryID) []models.StreamEntry {
	var res []models.StreamEntry

	streamStoreMtx.RLock()
	stream := streamStore[key]
	streamStoreMtx.RUnlock()

	stream.Mtx.Lock()
	defer stream.Mtx.Unlock()

	i := 0
	for ; i < len(stream.Entries) && (startingEntryID != stream.Entries[i].ID && startingEntryID.GreaterThan(stream.Entries[i].ID)); i++ {
	}

	for ; i < len(stream.Entries) && (endingEntryID == stream.Entries[i].ID || endingEntryID.GreaterThan(stream.Entries[i].ID)); i++ {
		res = append(res, stream.Entries[i])
	}

	return res
}

func readStreams(streams []models.ReadStream, timeout int) []models.StreamOutput {
	var res []models.StreamOutput
	if timeout > 0 {
		res = readStreamsBlocking(streams[0].StreamID, streams[0].StartEntryID, timeout)
	} else {
		for _, stream := range streams {
			strOut := models.StreamOutput{
				StreamID:      stream.StreamID,
				StreamEntries: getStreamEntries(stream.StreamID, stream.StartEntryID, MaxStreamID),
			}
			res = append(res, strOut)
		}
	}

	return res
}

func readStreamsBlocking(key string, startingEntryID models.StreamEntryID, timeout int) []models.StreamOutput {
	var res []models.StreamOutput

	streamStoreMtx.Lock()
	stream, exists := streamStore[key]
	if !exists {
		stream = &models.Stream{
			Mtx:     &sync.RWMutex{},
			Entries: []models.StreamEntry{},
		}
		streamStore[key] = stream
		keys[key] = "stream"
	}
	streamStoreMtx.Unlock()

	startingEntryID.Seq++

	if stream.HasEntriesAfter(startingEntryID) {
		res = append(res, models.StreamOutput{
			StreamID:      key,
			StreamEntries: getStreamEntries(key, startingEntryID, MaxStreamID),
		})
		return res
	}

	ch := make(chan struct{}, 1)
	stream.Mtx.Lock()
	stream.Waiters = append(stream.Waiters, ch)
	stream.Mtx.Unlock()

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(time.Duration(timeout) * time.Millisecond)
	}

	for {
		select {
		case <-ch:
			if stream != nil {
				removeWaiterFromStream(stream, ch)
				return []models.StreamOutput{
					{
						StreamID:      key,
						StreamEntries: getStreamEntries(key, startingEntryID, MaxStreamID),
					},
				}
			}

		case <-timeoutCh:
			removeWaiterFromStream(stream, ch)
			return nil
		}
	}
}
