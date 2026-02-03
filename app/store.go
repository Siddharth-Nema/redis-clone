package main

import (
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

var (
	keyStore    = store.NewKeyStore()
	stringStore = store.NewStringStore()
	listStore   = store.NewListStore()
	streamStore = store.NewStreamStore()
)
