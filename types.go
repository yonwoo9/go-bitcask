package bitcask

import (
	"errors"
	"os"
	"sync"
)

type Bitcask struct {
	directory    string
	activeFile   *os.File
	activeFileID int64
	keydir       map[string]entry
	mutex        sync.RWMutex
	config       *Config
	mmapedFiles  map[int64]*MmapedFile
}

type entry struct {
	valueSize int32
	valuePos  int64
	timestamp int64
	fileID    int64
}

type MmapedFile struct {
	data []byte
	file *os.File
}

const (
	headerSize = 20 // 4(crc) + 8(timestamp) + 4(keySize) + 4(valueSize)
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrIOFailure   = errors.New("I/O operation failed")
)
