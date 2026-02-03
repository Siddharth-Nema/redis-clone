package store

import (
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/models"
)

type StreamStore struct {
	store map[string]*models.Stream
	mtx   sync.RWMutex
}

func NewStreamStore() *StreamStore {
	return &StreamStore{
		store: make(map[string]*models.Stream),
	}
}

func (streamStore *StreamStore) createStream(key string) *models.Stream {
	streamStore.mtx.Lock()
	defer streamStore.mtx.Unlock()

	stream := &models.Stream{
		Mtx:     &sync.RWMutex{},
		Entries: []models.StreamEntry{},
	}
	streamStore.store[key] = stream
	return stream
}

func (streamStore *StreamStore) getOrCreateStream(key string) *models.Stream {
	streamStore.mtx.RLock()
	stream, exists := streamStore.store[key]
	streamStore.mtx.RUnlock()

	if exists {
		return stream
	}

	return streamStore.createStream(key)
}

func (streamStore *StreamStore) AddToStream(key string, entryID string, values []string) (string, error) {
	streamPtr := streamStore.getOrCreateStream(key)

	err := streamPtr.ValidateEntryID(entryID)

	if err != nil {
		return "", err
	}

	streamPtr.Mtx.Lock()
	defer streamPtr.Mtx.Unlock()

	var newEntry models.StreamEntry
	newEntry.ID, err = streamPtr.GenerateStreamIDFromString(entryID)
	if err != nil {
		return "", err
	}

	newEntry.Values = values

	streamPtr.Entries = append(streamPtr.Entries, newEntry)

	for _, ch := range streamPtr.Waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	return newEntry.ID.StreamIDToString(), nil
}

func (streamStore *StreamStore) GetStreamEntries(key string, startingEntryID models.StreamEntryID, endingEntryID models.StreamEntryID) []models.StreamEntry {
	var res []models.StreamEntry

	streamStore.mtx.RLock()
	stream := streamStore.store[key]
	streamStore.mtx.RUnlock()

	stream.Mtx.Lock()
	defer stream.Mtx.Unlock()

	i := 0
	for ; i < len(stream.Entries) && (startingEntryID != stream.Entries[i].ID && startingEntryID.GreaterThan(stream.Entries[i].ID)); i++ {
	}

	for ; i < len(stream.Entries) && (endingEntryID == stream.Entries[i].ID || endingEntryID.GreaterThan(stream.Entries[i].ID)); i++ {
		res = append(res, stream.Entries[i])
	}

	return res
}

func (streamStore *StreamStore) ReadStreams(streams []models.ReadStream, timeout int) []models.StreamOutput {
	var res []models.StreamOutput
	if timeout > 0 {
		res = streamStore.ReadStreamsBlocking(streams[0].StreamID, streams[0].StartEntryID, timeout)
	} else {
		for _, stream := range streams {
			strOut := models.StreamOutput{
				StreamID:      stream.StreamID,
				StreamEntries: streamStore.GetStreamEntries(stream.StreamID, stream.StartEntryID, models.MaxStreamID),
			}
			res = append(res, strOut)
		}
	}

	return res
}

func (streamStore *StreamStore) ReadStreamsBlocking(key string, startingEntryID models.StreamEntryID, timeout int) []models.StreamOutput {
	var res []models.StreamOutput

	streamStore.mtx.Lock()
	stream, exists := streamStore.store[key]
	if !exists {
		stream = &models.Stream{
			Mtx:     &sync.RWMutex{},
			Entries: []models.StreamEntry{},
		}
		streamStore.store[key] = stream
	}
	streamStore.mtx.Unlock()

	startingEntryID.Seq++

	if stream.HasEntriesAfter(startingEntryID) {
		res = append(res, models.StreamOutput{
			StreamID:      key,
			StreamEntries: streamStore.GetStreamEntries(key, startingEntryID, models.MaxStreamID),
		})
		return res
	}

	ch := make(chan struct{}, 1)
	stream.Mtx.Lock()
	stream.Waiters = append(stream.Waiters, ch)
	stream.Mtx.Unlock()

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(time.Duration(timeout) * time.Millisecond)
	}

	for {
		select {
		case <-ch:
			if stream != nil {
				stream.RemoveWaiterFromStream(ch)
				return []models.StreamOutput{
					{
						StreamID:      key,
						StreamEntries: streamStore.GetStreamEntries(key, startingEntryID, models.MaxStreamID),
					},
				}
			}

		case <-timeoutCh:
			stream.RemoveWaiterFromStream(ch)
			return nil
		}
	}
}
