package main

import (
	"container/list"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/models"
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
type ListData struct {
	mu        sync.RWMutex
	items     []string
	semaphore *Semaphore
	waiters   *list.List
}

var (
	listStore = make(map[string]*ListData)
	listMtx   sync.Mutex // Protects listStore map itself
)

// All keys Store
var (
	keys    = make(map[string]string)
	keysMtx sync.RWMutex
)

// Streams
var (
	streamStore    = make(map[string]*models.Stream)
	streamStoreMtx sync.RWMutex
)
