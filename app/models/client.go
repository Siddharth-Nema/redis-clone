package models

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var clientSeq uint64 = 0
var clientsMu sync.Mutex
var ClientList = make(map[uint64]*Client)

type Client struct {
	Id              uint64
	Conn            net.Conn
	Queue           [][]string
	InMulti         bool
	IsSlave         bool
	ListeningPort   string
	LastKnownOffset int
}

func NewClient(conn net.Conn) *Client {
	id := atomic.AddUint64(&clientSeq, 1)
	c := &Client{
		Id:              id,
		Conn:            conn,
		Queue:           [][]string{},
		InMulti:         false,
		IsSlave:         false,
		LastKnownOffset: 0,
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

func (c *Client) SendHeartbeat(timeout time.Duration) error {
	err := c.Conn.SetWriteDeadline(time.Now().Add(timeout))
	if err != nil {
		return err
	}

	_, err = c.Conn.Write([]byte("+PING\r\n"))

	if err != nil {
		return fmt.Errorf("heartbeat failed: %w", err)
	}

	return c.Conn.SetWriteDeadline(time.Time{})
}
