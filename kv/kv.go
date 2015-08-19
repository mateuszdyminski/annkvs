package kv

// KeyValueDB set of key-value db methods.
type KeyValueDB interface {
	Open()
	Insert(string, string)
	Get(string) (string, bool)
	Close()
}
