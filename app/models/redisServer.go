package models

import (
	"sync"
)

type RedisServer struct {
	Role             string
	Port             string
	MasterHost       string
	MasterPort       string
	MasterReplID     string
	masterReplOffset int
	offsetMtx        sync.RWMutex
	mu               sync.RWMutex // Protects the fields above
	replicasList     []*Client
	replicaMtx       sync.RWMutex
	DirPath          string
	DbFilename       string
}

func NewRedisServer() *RedisServer {
	return &RedisServer{
		Role:             "master",
		Port:             "6379",
		masterReplOffset: 0,
		replicasList:     []*Client{},
	}
}

func (s *RedisServer) GetOffset() int {
	s.offsetMtx.RLock()
	defer s.offsetMtx.RUnlock()

	return s.masterReplOffset
}

func (s *RedisServer) AddToOffset(val int) {
	s.offsetMtx.RLock()
	defer s.offsetMtx.RUnlock()

	s.masterReplOffset += val
}

func (s *RedisServer) IsMaster() bool {
	return s.Role == "master"
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
