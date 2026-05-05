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
	channel, exists := store.channels[key]
	if exists {
		return channel
	} else {
		store.channels[key] = &models.Channel{
			Name:        key,
			Publishers:  []*models.Client{},
			Subscribers: []*models.Client{},
		}
		return store.channels[key]
	}
}
