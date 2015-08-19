package v1

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/mateuszdyminski/annkvs/kv"
)

// Kv - Key Value db struct
type Kv struct {
	filePath   string
	file       *os.File
	writeIndex int64
	index      map[string]value
}

type value struct {
	offset int64
	size   int
}

// NewKv creates key value db
func NewKv(filePath string) kv.KeyValueDB {
	return &Kv{filePath: filePath, index: make(map[string]value)}
}

// Open opens.
func (k *Kv) Open() {
	var err error
	if k.file, err = os.OpenFile(k.filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666); err != nil {
		panic(err)
	}

	stat, err := k.file.Stat()
	if err != nil {
		panic(err)
	}

	k.writeIndex = stat.Size()
}

// Insert inserts.
func (k *Kv) Insert(key string, val string) {
	n, err := k.file.WriteString(fmt.Sprintf("%s,%s\n", key, val))
	if err != nil {
		panic(err)
	}

	k.index[key] = value{offset: k.writeIndex, size: n}
	k.writeIndex += int64(n)
}

// Get gets.
func (k *Kv) Get(key string) (string, bool) {
	desc, ok := k.index[key]
	if ok {
		value := make([]byte, desc.size)
		_, err := k.file.ReadAt(value, desc.offset)
		if err != nil {
			panic(err)
		}

		return strings.Split(string(value), ",")[1], true
	}

	val, err := k.scanFile(key)
	if err != nil {
		return "", false
	}

	return val, true
}

func (k *Kv) scanFile(key string) (string, error) {
	k.file.Seek(0, 0)
	scanner := bufio.NewScanner(k.file)
	var val string
	for scanner.Scan() {
		kv := strings.Split(scanner.Text(), ",")
		if kv[0] == key {
			val = kv[1]
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return val, nil
}

// Close closes.
func (k *Kv) Close() {
	k.file.Close()
}
