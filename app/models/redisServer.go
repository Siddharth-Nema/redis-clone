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
}

func NewRedisServer() *RedisServer {
	return &RedisServer{
		Role:             "master",
		Port:             "6379",
		MasterReplOffset: 0,
	}
}
