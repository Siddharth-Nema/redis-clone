package models

import "sync"

type RedisServer struct {
	Role             string
	Port             string
	MasterHost       string
	MasterPort       string
	MasterReplID     string
	MasterReplOffset int
	mu               sync.RWMutex // Protects the fields above
	replicasList     []*Client
	replicaMtx       sync.RWMutex
}

func NewRedisServer() *RedisServer {
	return &RedisServer{
		Role:             "master",
		Port:             "6379",
		MasterReplOffset: 0,
		replicasList:     []*Client{},
	}
}

func (s *RedisServer) GetReplicas() []*Client {
	s.replicaMtx.RLock()
	defer s.replicaMtx.RUnlock()

	replicas := make([]*Client, len(s.replicasList))
	copy(replicas, s.replicasList)
	return replicas

}

func (s *RedisServer) AddReplica(client *Client) {
	s.replicaMtx.Lock()
	defer s.replicaMtx.Unlock()
	s.replicasList = append(s.replicasList, client)
}
