package v2

import "github.com/mateuszdyminski/annkvs/kv"

// Kv - Key Value db struct
type Kv struct {
	storagePath string
	storage     *storage
}

// NewKv creates key value db
func NewKv(path string) kv.KeyValueDB {
	return &Kv{storagePath: path}
}

// Open opens.
func (k *Kv) Open() {
	k.storage = newStorage(k.storagePath)
}

// Insert inserts.
func (k *Kv) Insert(key string, val string) {
	k.storage.insert(key, val)
}

// Get gets.
func (k *Kv) Get(key string) (string, bool) {
	return k.storage.get(key)
}

// Close closes.
func (k *Kv) Close() {

}
