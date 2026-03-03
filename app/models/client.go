package models

import (
	"net"
	"sync"
	"sync/atomic"
)

var clientSeq uint64 = 0
var clientsMu sync.Mutex
var ClientList = make(map[uint64]*Client)

type Client struct {
	Id      uint64
	Conn    net.Conn
	Queue   [][]string
	InMulti bool
}

func NewClient(conn net.Conn) *Client {
	id := atomic.AddUint64(&clientSeq, 1)
	c := &Client{
		Id:      id,
		Conn:    conn,
		Queue:   [][]string{},
		InMulti: false,
	}
	clientsMu.Lock()
	ClientList[id] = c
	clientsMu.Unlock()
	return c
}

func (client *Client) Close() {
	client.Conn.Close()
	clientsMu.Lock()
	delete(ClientList, client.Id)
	clientsMu.Unlock()
}
