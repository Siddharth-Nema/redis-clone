package store

import "sync"

type KeyStore struct {
	keys map[string]string
	mtx  sync.RWMutex
}

func NewKeyStore() *KeyStore {
	return &KeyStore{
		keys: make(map[string]string),
	}
}

func (keyStore *KeyStore) GetType(key string) string {
	keyStore.mtx.RLock()
	defer keyStore.mtx.RUnlock()
	keyType, exists := keyStore.keys[key]

	if exists {
		return keyType
	}

	return "none"
}

func (keyStore *KeyStore) SetType(key string, keyType string) {
	keyStore.mtx.Lock()
	defer keyStore.mtx.Unlock()
	keyStore.keys[key] = keyType
}

func (keyStore *KeyStore) Delete(key string) bool {
	keyStore.mtx.Lock()
	defer keyStore.mtx.Unlock()

	_, exists := keyStore.keys[key]
	if exists {
		delete(keyStore.keys, key)
	}
	return exists
}
