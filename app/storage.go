package main

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// String Store
var (
	store    = make(map[string]string)
	storeMtx sync.RWMutex
)

// String expiry store
var (
	expiry    = make(map[string]time.Time)
	expiryMtx sync.RWMutex
)

// List Store
var (
	listStore      = make(map[string][]string)
	listMutexes    = make(map[string]*sync.RWMutex)
	listSemaphores = make(map[string]*Semaphore)
	waiters        = make(map[string]*list.List)
	listMutexesMtx sync.Mutex
)

// All keys Store
var (
	keys    = make(map[string]string)
	keysMtx sync.RWMutex
)

// Streams
var (
	streamStore    = make(map[string]*Stream)
	streamStoreMtx sync.RWMutex
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

func getListMutex(key string) *sync.RWMutex {
	listMutexesMtx.Lock()
	defer listMutexesMtx.Unlock()

	if _, exists := listMutexes[key]; !exists {
		listMutexes[key] = &sync.RWMutex{}
	}
	return listMutexes[key]
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

	return nil
}

func createListIfDoesntExist(key string) {
	listMutexesMtx.Lock()
	defer listMutexesMtx.Unlock()

	if _, exists := listStore[key]; !exists {
		listStore[key] = []string{}
		setType(key, "list")
	}
	if _, exists := listSemaphores[key]; !exists {
		listSemaphores[key] = NewSemaphore()
	}
	if _, exists := waiters[key]; !exists {
		waiters[key] = list.New()
	}
}

func pushToList(key string, vals []string) int {
	mtx := getListMutex(key)
	mtx.Lock()
	defer mtx.Unlock()
	createListIfDoesntExist(key)

	sem := listSemaphores[key]
	wq := waiters[key]

	newLen := len(vals) + len(listStore[key])

	i := 0
	for i < len(vals) && wq.Len() > 0 {
		e := wq.Front()
		w := e.Value.(*waiter)
		wq.Remove(e)

		w.ch <- result{val: vals[i], ok: true}
		i++
	}

	if i < len(vals) {
		listStore[key] = append(listStore[key], vals[i:]...)
		sem.ReleaseN(len(vals) - i)
	}

	return newLen
}

func prependToList(key string, vals []string) int {
	mtx := getListMutex(key)
	mtx.Lock()
	defer mtx.Unlock()

	createListIfDoesntExist(key)
	sem := listSemaphores[key]
	wq := waiters[key]

	i := len(vals) - 1
	for i >= 0 && wq.Len() > 0 {
		e := wq.Front()
		w := e.Value.(*waiter)
		wq.Remove(e)

		w.ch <- result{val: vals[i], ok: true}
		i--
	}

	if i >= 0 {
		remaining := vals[:i+1]
		listStore[key] = append(vals, listStore[key]...)

		sem.ReleaseN(len(remaining))
	}

	return len(listStore[key])

}

func getItemsFromList(key string, start int, end int) []string {
	mtx := getListMutex(key)
	mtx.RLock()
	defer mtx.RUnlock()

	reqList := listStore[key]
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
	createListIfDoesntExist(key)

	listMtx := getListMutex(key)
	listMtx.Lock()

	// If list has items, pop immediately
	if len(listStore[key]) > 0 {
		val := listStore[key][0]
		listStore[key] = listStore[key][1:]
		listMtx.Unlock()
		return val, true
	}

	// List is empty - create waiter and add to queue
	w := &waiter{ch: make(chan result, 1)}
	elem := waiters[key].PushBack(w)
	listMtx.Unlock()

	// Wait for result on the channel
	if timeoutSecs <= 0 {
		res := <-w.ch
		return res.val, res.ok
	} else {
		timeout := time.Duration(timeoutSecs * float64(time.Second))
		select {
		case res := <-w.ch:
			return res.val, res.ok
		case <-time.After(timeout):
			// Timeout - remove waiter from queue
			listMtx.Lock()
			waiters[key].Remove(elem)
			listMtx.Unlock()
			return "", false
		}
	}
}

func getLength(key string) int {
	mtx := getListMutex(key)
	mtx.RLock()
	defer mtx.RUnlock()

	return len(listStore[key])
}

func popFromLeftofArray(key string, count int) ([]string, bool) {
	mtx := getListMutex(key)
	mtx.Lock()
	defer mtx.Unlock()

	if len(listStore[key]) >= count {
		val := listStore[key][0:count]
		listStore[key] = listStore[key][count:]
		return val, true
	} else {
		return []string{}, false
	}
}

func createStream(key string) {
	// create stream entry and set key type
	streamStoreMtx.Lock()
	defer streamStoreMtx.Unlock()

	streamStore[key] = &Stream{
		mtx:     &sync.RWMutex{},
		entries: []StreamEntry{},
	}

	keysMtx.Lock()
	keys[key] = "stream"
	keysMtx.Unlock()
}

func addToStream(key string, entryID string, values map[string]string) error {
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
		return err
	}

	streamPtr.mtx.Lock()
	defer streamPtr.mtx.Unlock()

	var newEntry StreamEntry
	newEntry.ID = entryID
	newEntry.Values = values

	streamPtr.entries = append(streamPtr.entries, newEntry)
	return nil
}
