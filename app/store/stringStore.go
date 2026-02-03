package store

import (
	"sync"
	"time"
)

type StringStore struct {
	store   map[string]string
	expiry  map[string]time.Time
	mtx     sync.RWMutex
	expiryM sync.RWMutex
}

func NewStringStore() *StringStore {
	return &StringStore{
		store:  make(map[string]string),
		expiry: make(map[string]time.Time),
	}
}

func (stringStore *StringStore) Set(key string, val string) {
	stringStore.mtx.Lock()
	defer stringStore.mtx.Unlock()
	stringStore.store[key] = val
}

func (stringStore *StringStore) Get(key string) (string, bool) {
	stringStore.mtx.RLock()
	defer stringStore.mtx.RUnlock()

	val, ok := stringStore.store[key]
	return val, ok
}

func (stringStore *StringStore) Delete(key string) bool {
	stringStore.mtx.Lock()
	defer stringStore.mtx.Unlock()

	_, exists := stringStore.store[key]
	if exists {
		delete(stringStore.store, key)
	}

	stringStore.expiryM.Lock()
	delete(stringStore.expiry, key)
	stringStore.expiryM.Unlock()

	return exists
}

func (stringStore *StringStore) SetExpiry(key string, timestamp time.Time) {
	stringStore.expiryM.Lock()
	defer stringStore.expiryM.Unlock()

	stringStore.expiry[key] = timestamp
}

func (stringStore *StringStore) GetExpiry(key string) time.Time {
	stringStore.expiryM.RLock()
	defer stringStore.expiryM.RUnlock()

	val, ok := stringStore.expiry[key]
	if ok {
		return val
	}
	return time.Time{}
}

func (stringStore *StringStore) ClearExpiry(key string) {
	stringStore.expiryM.Lock()
	defer stringStore.expiryM.Unlock()

	delete(stringStore.expiry, key)
}

func (stringStore *StringStore) IsExpired(key string, now time.Time) bool {
	exp := stringStore.GetExpiry(key)
	if exp.IsZero() {
		return false
	}
	return now.After(exp)
}
