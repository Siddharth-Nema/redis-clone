package models

import "sync"

type Publisher struct {
	client Client
}

type Subscriber struct {
	client Client
}

type Channel struct {
	publishers  []Client
	subscribers []Client
	mtx         sync.Mutex
}

func (channel *Channel) Publish(msg string) {
	channel.mtx.Lock()
	defer channel.mtx.Unlock()

	// for _, subscriber := range channel.subscribers {
	// 	subscriber.Conn.Write([]byte(convertToRESPString(msg)))
	// }
}

func (channel *Channel) AddToSubscribers(newClient *Client) {
	channel.mtx.Lock()
	defer channel.mtx.Unlock()

	channel.subscribers = append(channel.subscribers, *newClient)
}
