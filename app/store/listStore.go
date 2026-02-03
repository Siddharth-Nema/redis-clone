package store

import (
	"container/list"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

type ListStore struct {
	store map[string]*models.ListData
	mtx   sync.RWMutex
}

func NewListStore() *ListStore {
	return &ListStore{
		store: make(map[string]*models.ListData),
	}
}

func (listStore *ListStore) getOrCreateList(key string) *models.ListData {
	listStore.mtx.Lock()
	defer listStore.mtx.Unlock()

	if _, exists := listStore.store[key]; !exists {
		listStore.store[key] = &models.ListData{
			Items:     []string{},
			Semaphore: models.NewSemaphore(),
			Waiters:   list.New(),
		}
	}
	return listStore.store[key]
}

func (listStore *ListStore) Delete(key string) bool {
	listStore.mtx.Lock()
	defer listStore.mtx.Unlock()

	_, exists := listStore.store[key]
	if exists {
		delete(listStore.store, key)
	}
	return exists
}

func (listStore *ListStore) PushToList(key string, vals []string) int {
	ld := listStore.getOrCreateList(key)
	ld.Mtx.Lock()
	defer ld.Mtx.Unlock()

	newLen := len(vals) + len(ld.Items)

	i := 0
	for i < len(vals) && ld.Waiters.Len() > 0 {
		e := ld.Waiters.Front()
		w := e.Value.(*models.Waiter)
		ld.Waiters.Remove(e)

		w.Ch <- models.Result{Val: vals[i], Ok: true}
		i++
	}

	if i < len(vals) {
		ld.Items = append(ld.Items, vals[i:]...)
		ld.Semaphore.ReleaseN(len(vals) - i)
	}

	return newLen
}

func (listStore *ListStore) PrependToList(key string, vals []string) int {
	ld := listStore.getOrCreateList(key)
	ld.Mtx.Lock()
	defer ld.Mtx.Unlock()

	i := len(vals) - 1
	for i >= 0 && ld.Waiters.Len() > 0 {
		e := ld.Waiters.Front()
		w := e.Value.(*models.Waiter)
		ld.Waiters.Remove(e)

		w.Ch <- models.Result{Val: vals[i], Ok: true}
		i--
	}

	if i >= 0 {
		remaining := vals[:i+1]
		ld.Items = append(vals, ld.Items...)

		ld.Semaphore.ReleaseN(len(remaining))
	}

	return len(ld.Items)
}

func (listStore *ListStore) GetItemsFromList(key string, start int, end int) []string {
	ld := listStore.getOrCreateList(key)
	ld.Mtx.RLock()
	defer ld.Mtx.RUnlock()

	reqList := ld.Items
	size := len(reqList)
	if start < 0 {
		start += size
	}
	if end < 0 {
		end += size
	}

	start = max(start, 0)
	end = min(end, size-1)

	if start > end || start > len(reqList) {
		return []string{}
	}

	return reqList[start : end+1]
}

func (listStore *ListStore) BlockingLPop(key string, timeoutSecs float64) (string, bool) {
	ld := listStore.getOrCreateList(key)
	ld.Mtx.Lock()

	if len(ld.Items) > 0 {
		val := ld.Items[0]
		ld.Items = ld.Items[1:]
		ld.Mtx.Unlock()
		return val, true
	}

	w := &models.Waiter{Ch: make(chan models.Result, 1)}
	elem := ld.Waiters.PushBack(w)
	ld.Mtx.Unlock()

	if timeoutSecs <= 0 {
		res := <-w.Ch
		return res.Val, res.Ok
	}

	timeout := time.Duration(timeoutSecs * float64(time.Second))
	select {
	case res := <-w.Ch:
		return res.Val, res.Ok
	case <-time.After(timeout):
		ld.Mtx.Lock()
		ld.Waiters.Remove(elem)
		ld.Mtx.Unlock()
		return "", false
	}
}

func (listStore *ListStore) GetLength(key string) int {
	ld := listStore.getOrCreateList(key)
	ld.Mtx.RLock()
	defer ld.Mtx.RUnlock()

	return len(ld.Items)
}

func (listStore *ListStore) PopFromLeftOfArray(key string, count int) ([]string, bool) {
	ld := listStore.getOrCreateList(key)
	ld.Mtx.Lock()
	defer ld.Mtx.Unlock()

	if len(ld.Items) >= count {
		val := ld.Items[0:count]
		ld.Items = ld.Items[count:]
		return val, true
	}
	return []string{}, false
}
