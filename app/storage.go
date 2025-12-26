package main

import "sync"

var (
	store = make(map[string]string)
	mu    sync.RWMutex
)

func set(key string, val string) {
	mu.Lock()
	defer mu.Unlock()
	store[key] = val
}

func get(key string) (string, bool) {
	mu.Lock()
	defer mu.Unlock()

	val, ok := store[key]
	return val, ok
}
