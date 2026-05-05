package models

import (
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
