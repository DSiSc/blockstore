package dbstore

// the max size of the batch
const MaxBatchSize = 100 * 1024

// Putter wraps the database write operation supported by both batches and regular databases.
type DBPutter interface {
	Put(key []byte, value []byte) error
}

// Deleter wraps the database delete operation supported by both batches and regular databases.
type DBDeleter interface {
	Delete(key []byte) error
}

// DBStore represent the low level database to store block
type DBStore interface {
	DBPutter
	DBDeleter
	// Get get from db
	Get(key []byte) ([]byte, error)
	// NewBatch create db batch
	NewBatch() Batch
}

// Batch is a write-only database that commits changes to its host database
// when Write is called. Batch cannot be used concurrently.
type Batch interface {
	DBPutter
	DBDeleter
	ValueSize() int // amount of data in the batch
	Write() error
	// Reset resets the batch for reuse
	Reset()
}
