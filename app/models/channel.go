package models

import (
	"slices"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/io"
)

type Channel struct {
	Name        string
	Publishers  []*Client
	Subscribers []*Client
	mtx         sync.Mutex
}

func (channel *Channel) Publish(msg string) {
	channel.mtx.Lock()
	defer channel.mtx.Unlock()

	for _, subscriber := range channel.Subscribers {
		subscriber.Send(io.ConvertToRESPArray([]string{"message", channel.Name, msg}))
	}
}

func (channel *Channel) AddClientAsSubscriber(client *Client) {
	channel.mtx.Lock()
	defer channel.mtx.Unlock()

	channel.Subscribers = append(channel.Subscribers, client)
}

func (channel *Channel) RemoveClientFromSubscribers(client *Client) {
	channel.mtx.Lock()
	defer channel.mtx.Unlock()

	var reqIdx int
	for idx, sub := range channel.Subscribers {
		if client.Id == sub.Id {
			reqIdx = idx
		}
	}

	channel.Subscribers = slices.Delete(channel.Subscribers, reqIdx, reqIdx+1)
}
