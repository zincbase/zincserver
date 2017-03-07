package main

// Declare the datastore simulator object
type ServerDatastoreHandlerSimulator struct {
	entries []Entry
}

// Datastore simulator object constructor
func NewServerDatastoreHandlerSimulator() *ServerDatastoreHandlerSimulator {
	return &ServerDatastoreHandlerSimulator{
		entries: nil,
	}
}

// Simulate a PUT request. The given entries must include the head entry.
func (this *ServerDatastoreHandlerSimulator) Put(entries []Entry) error {
	this.entries = append([]Entry{}, entries...)
	return nil
}

// Simulate a POST request.
func (this *ServerDatastoreHandlerSimulator) Post(entries []Entry) error {
	if this.entries == nil {
		return ErrNotFound
	}

	if entries == nil || len(entries) == 0 {
		return ErrEmptyTransaction
	}

	this.entries = append(this.entries, entries...)
	return nil
}

// Simulate a GET request.
func (this *ServerDatastoreHandlerSimulator) Get(updatedAfter int64) ([]Entry, error) {
	if this.entries == nil {
		return nil, ErrNotFound
	}

	for i := 0; i < len(this.entries); i++ {
		if this.entries[i].Header.CommitTime > updatedAfter {
			return this.entries[i:], nil
		}
	}

	return []Entry{}, nil
}

// Simulate a GET request and compact the result
func (this *ServerDatastoreHandlerSimulator) GetAndCompact(updatedAfter int64) ([]Entry, error) {
	results, err := this.Get(updatedAfter)
	if err != nil {
		return nil, err
	}

	return CompactEntries(results), nil
}

// Simulate a DELETE request.
func (this *ServerDatastoreHandlerSimulator) Delete() error {
	if this.entries == nil {
		return ErrNotFound
	}

	this.entries = nil

	return nil
}

// Get a random timestamp in the range of the commited entries +/- 1
func (this *ServerDatastoreHandlerSimulator) GetRandomTimestampInCommittedRange() int64 {
	if this.entries == nil || len(this.entries) == 0 {
		return 0
	}

	minTimestamp := this.entries[0].Header.CommitTime
	maxTimestamp := this.entries[len(this.entries)-1].Header.CommitTime
	return RandomInt63InRange(minTimestamp-1, maxTimestamp+1)
}

// Get a random timestamp in the range of the commited entries +/- 1
func (this *ServerDatastoreHandlerSimulator) GetRandomExistingKey() []byte {
	if this.entries == nil || len(this.entries) <= 1 {
		return nil
	}

	return this.entries[RandomIntInRange(1, len(this.entries))].Key
}

// Takes a series of entries and randomly replaces some of them with random entries with keys
// that already exist in the datastore
func (this *ServerDatastoreHandlerSimulator) ReplaceRandomEntriesWithExistingKeyedRandomEntries(entries []Entry) {
	if len(entries) <= 1 {
		return
	}

	mutationCount := RandomIntInRange(0, len(entries))

	for i := 0; i < mutationCount; i++ {
		existingKey := this.GetRandomExistingKey()

		if existingKey != nil {
			randomIndex := RandomIntInRange(1, len(entries))
			mutatedEntry := &entries[randomIndex]
			mutatedEntry.Key = existingKey
			mutatedEntry.Value = RandomBytes(len(mutatedEntry.Value) + RandomIntInRange(0, 20))
		}
	}
}
