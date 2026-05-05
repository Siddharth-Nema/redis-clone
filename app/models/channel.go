package models

import (
	"sync"
)

type Channel struct {
	Publishers  []*Client
	Subscribers []*Client
	mtx         sync.Mutex
}

func (channel *Channel) Publish(msg string) {
	channel.mtx.Lock()
	defer channel.mtx.Unlock()

	// for _, subscriber := range channel.subscribers {
	// 	subscriber.Conn.Write([]byte(convertToRESPString(msg)))
	// }
}

func (channel *Channel) AddClientAsSubscriber(client *Client) {
	channel.mtx.Lock()
	defer channel.mtx.Unlock()

	channel.Subscribers = append(channel.Subscribers, client)
}
