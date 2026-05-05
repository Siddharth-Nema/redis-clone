package store

import (
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

type ChannelStore struct {
	channels map[string]*models.Channel
	mtx      sync.RWMutex
}

func NewChannelStore() *ChannelStore {
	return &ChannelStore{
		channels: make(map[string]*models.Channel),
	}
}

func (store *ChannelStore) Get(key string) *models.Channel {
	store.mtx.RLock()
	defer store.mtx.RUnlock()

	if channel, exists := store.channels[key]; exists {
		return channel
	} else {
		return &models.Channel{}
	}
}
