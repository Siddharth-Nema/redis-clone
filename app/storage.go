package main

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

var (
	store    = make(map[string]string)
	storeMtx sync.RWMutex
)

var (
	listStore      = make(map[string][]string)
	listMutexes    = make(map[string]*sync.RWMutex)
	listSemaphores = make(map[string]*Semaphore)
	waiters        = make(map[string]*list.List)
	listMutexesMtx sync.Mutex
)

var (
	expiry    = make(map[string]time.Time)
	expiryMtx sync.RWMutex
)

func set(key string, val string) {
	storeMtx.Lock()
	defer storeMtx.Unlock()
	store[key] = val
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

func createIfDoesNotExist(key string) {
	listMutexesMtx.Lock()
	defer listMutexesMtx.Unlock()

	if _, exists := listStore[key]; !exists {
		listStore[key] = []string{}
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
	createIfDoesNotExist(key)

	sem := listSemaphores[key]
	wq := waiters[key]

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

	return len(listStore[key])
}

func prependToList(key string, vals []string) int {
	mtx := getListMutex(key)
	mtx.Lock()
	defer mtx.Unlock()

	createIfDoesNotExist(key)
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

func blockingLPop(key string, timeoutSecs int) (string, bool) {
	// ensure structures exist for the key
	createIfDoesNotExist(key)

	// If timeoutSecs <= 0, wait forever; otherwise wait with timeout
	if timeoutSecs <= 0 {
		listSemaphores[key].Acquire()
	} else {
		timeout := time.Duration(timeoutSecs) * time.Second
		ok := listSemaphores[key].AcquireTimeout(timeout)
		if !ok {
			return "", false
		}
	}

	listMtx := getListMutex(key)
	listMtx.Lock()
	defer listMtx.Unlock()

	// guaranteed by semaphore
	val := listStore[key][0]
	listStore[key] = listStore[key][1:]
	return val, true
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
