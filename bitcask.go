package bitcask

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Open opens a Bitcask database instance.
func Open(dir string, opts ...ConfOption) (*Bitcask, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	config := DefaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	b := &Bitcask{
		directory:   dir,
		keydir:      make(map[string]entry),
		config:      config,
		mmapedFiles: make(map[int64]*MmapedFile),
	}

	// 加载现有的数据文件
	if err := b.loadExistingFiles(); err != nil {
		return nil, fmt.Errorf("failed to load existing files: %w", err)
	}

	// 如果没有现有的数据文件，创建一个新的
	if b.activeFileID == 0 {
		if err := b.openNewActiveFile(); err != nil {
			return nil, fmt.Errorf("failed to open new active file: %w", err)
		}
	} else {
		// 打开最后一个数据文件作为活动文件
		if err := b.openActiveFile(b.activeFileID); err != nil {
			return nil, fmt.Errorf("failed to open active file: %w", err)
		}
		// 更新内存映射
		if err := b.updateMmap(b.activeFileID); err != nil {
			return nil, fmt.Errorf("failed to update mmap: %w", err)
		}
	}

	go b.periodicMerge()

	return b, nil
}

// Put inserts a key-value pair into the Bitcask database.
func (b *Bitcask) Put(key string, value []byte) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.put(key, value)
}

func (b *Bitcask) put(key string, value []byte) error {
	timestamp := time.Now().UnixNano()
	keySize := len(key)
	valueSize := len(value)

	if b.config.CompressData {
		var buf bytes.Buffer
		w := zlib.NewWriter(&buf)
		if _, err := w.Write(value); err != nil {
			return fmt.Errorf("failed to compress data: %w", err)
		}
		w.Close()
		value = buf.Bytes()
		valueSize = len(value)
	}

	header := make([]byte, headerSize)
	binary.BigEndian.PutUint64(header[4:], uint64(timestamp))
	binary.BigEndian.PutUint32(header[12:], uint32(keySize))
	binary.BigEndian.PutUint32(header[16:], uint32(valueSize))

	crc := crc32.ChecksumIEEE(append([]byte(key), value...))
	binary.BigEndian.PutUint32(header[:4], crc)

	totalSize := int64(headerSize) + int64(keySize) + int64(valueSize)

	// 检查是否需要创建新文件
	if b.activeFile == nil || b.getActiveFileSize()+totalSize > b.config.MaxFileSize {
		if err := b.openNewActiveFile(); err != nil {
			return fmt.Errorf("failed to open new active file: %w", err)
		}
		// 创建新的hint文件
		hintFilename := b.getHintFilePath(b.activeFileID)
		hintFile, err := os.Create(hintFilename)
		if err != nil {
			return fmt.Errorf("failed to create hint file: %w", err)
		}
		defer hintFile.Close()

		// 将当前keydir中的所有条目写入新的hint文件
		for k, e := range b.keydir {
			if err := b.writeHintEntry(hintFile, k, e); err != nil {
				return fmt.Errorf("failed to write hint entry: %w", err)
			}
		}
	}

	valuePos, err := b.activeFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}

	// 写入数据
	if _, err := b.activeFile.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if _, err := b.activeFile.Write([]byte(key)); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}
	if _, err := b.activeFile.Write(value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}

	if b.config.SyncWrites {
		if err := b.activeFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
	}

	b.keydir[key] = entry{
		fileID:    b.activeFileID,
		valueSize: int32(valueSize),
		valuePos:  valuePos + headerSize + int64(keySize),
		timestamp: timestamp,
	}

	// 更新内存映射
	if err := b.updateMmap(b.activeFileID); err != nil {
		return fmt.Errorf("failed to update mmap: %w", err)
	}

	return nil
}

// Get retrieves the value associated with a given key from the Bitcask database.
func (b *Bitcask) Get(key string) ([]byte, error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	e, ok := b.keydir[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	mf, ok := b.mmapedFiles[e.fileID]
	if !ok {
		return nil, fmt.Errorf("mmap file not found for file ID %d", e.fileID)
	}

	if e.valuePos+int64(e.valueSize) > int64(len(mf.data)) {
		return nil, fmt.Errorf("value position out of range")
	}

	value := mf.data[e.valuePos : e.valuePos+int64(e.valueSize)]

	if b.config.CompressData {
		r, err := zlib.NewReader(bytes.NewReader(value))
		if err != nil {
			return nil, fmt.Errorf("failed to create zlib reader: %w", err)
		}
		defer r.Close()

		var buf bytes.Buffer
		if _, err := io.Copy(&buf, r); err != nil {
			return nil, fmt.Errorf("failed to decompress data: %w", err)
		}
		value = buf.Bytes()
	}

	return value, nil
}

// Delete removes a key-value pair from the Bitcask database.
func (b *Bitcask) Delete(key string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// 写入一个特殊的墓碑值
	if err := b.put(key, []byte{}); err != nil {
		return fmt.Errorf("failed to write tombstone: %w", err)
	}

	// 从keydir中删除
	delete(b.keydir, key)
	return nil
}

// BatchPut inserts multiple key-value pairs into the Bitcask database.
func (b *Bitcask) BatchPut(pairs map[string][]byte) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for key, value := range pairs {
		if err := b.put(key, value); err != nil {
			return fmt.Errorf("failed to put key %s: %w", key, err)
		}
	}
	return nil
}

// BatchGet retrieves multiple key-value pairs from the Bitcask database.
func (b *Bitcask) BatchGet(keys []string) (map[string][]byte, error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	result := make(map[string][]byte)
	for _, key := range keys {
		value, err := b.Get(key)
		if err == nil {
			result[key] = value
		} else if !errors.Is(err, ErrKeyNotFound) {
			return nil, fmt.Errorf("failed to get key %s: %w", key, err)
		}
	}
	return result, nil
}

// Iterator creates an iterator over the key-value pairs in the Bitcask database.
func (b *Bitcask) Iterator() *Iterator {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	keys := make([]string, 0, len(b.keydir))
	for k := range b.keydir {
		keys = append(keys, k)
	}

	return &Iterator{
		bitcask: b,
		keys:    keys,
		index:   0,
	}
}

// Snapshot creates a snapshot of the current state of the Bitcask database.
func (b *Bitcask) Snapshot(snapshotDir string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	for fileID := range b.mmapedFiles {
		srcPath := b.getDataFilePath(fileID)
		dstPath := filepath.Join(snapshotDir, filepath.Base(srcPath))

		if err := b.copyFile(srcPath, dstPath); err != nil {
			return fmt.Errorf("failed to copy data file: %w", err)
		}

		hintSrcPath := b.getHintFilePath(fileID)
		hintDstPath := filepath.Join(snapshotDir, filepath.Base(hintSrcPath))

		if err := b.copyFile(hintSrcPath, hintDstPath); err != nil {
			return fmt.Errorf("failed to copy hint file: %w", err)
		}
	}

	return nil
}

// Close closes the Bitcask database, ensuring all files are properly closed and memory maps are unmapped.
func (b *Bitcask) Close() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.activeFile != nil {
		// 创建最后的hint文件
		hintFilename := b.getHintFilePath(b.activeFileID)
		hintFile, err := os.Create(hintFilename)
		if err != nil {
			return fmt.Errorf("failed to create final hint file: %w", err)
		}
		defer hintFile.Close()

		// 将当前keydir中的所有条目写入hint文件
		for k, e := range b.keydir {
			if err := b.writeHintEntry(hintFile, k, e); err != nil {
				return fmt.Errorf("failed to write final hint entry: %w", err)
			}
		}

		if err := b.activeFile.Close(); err != nil {
			return fmt.Errorf("failed to close active file: %w", err)
		}
	}

	for _, mf := range b.mmapedFiles {
		if err := b.unmmapFile(mf); err != nil {
			return fmt.Errorf("failed to unmap file: %w", err)
		}
	}

	return nil
}
