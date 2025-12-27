package main

import (
	"fmt"
	"sync"
	"time"
)

var (
	store    = make(map[string]string)
	storeMtx sync.RWMutex
)

var (
	listStore    = make(map[string][]string)
	listStoreMtx sync.RWMutex
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

func pushToList(key string, vals []string) int {
	listStoreMtx.Lock()
	defer listStoreMtx.Unlock()

	for _, val := range vals {
		listStore[key] = append(listStore[key], val)
	}
	return len(listStore[key])
}
