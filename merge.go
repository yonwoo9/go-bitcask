package bitcask

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// periodicMerge periodically merges the database into a new file.
func (b *Bitcask) periodicMerge() {
	ticker := time.NewTicker(b.config.MergeInterval)
	defer ticker.Stop()

	for range ticker.C {
		b.merge()
	}
}

// merge 合并数据文件
func (b *Bitcask) merge() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// 获取所有数据文件
	dataFiles, err := filepath.Glob(filepath.Join(b.directory, "*.data"))
	if err != nil {
		return err
	}

	if len(dataFiles) < b.config.MergeThreshold {
		return nil
	}

	// 创建新的合并文件
	mergedFileID := time.Now().UnixNano()
	mergedFilename := b.getDataFilePath(mergedFileID)
	mergedFile, err := os.Create(mergedFilename)
	if err != nil {
		return err
	}
	defer mergedFile.Close()

	mergedHintFilename := b.getHintFilePath(mergedFileID)
	mergedHintFile, err := os.Create(mergedHintFilename)
	if err != nil {
		return err
	}
	defer mergedHintFile.Close()

	// 遍历所有key，写入最新的值到新文件
	for key, record := range b.keydir {
		if record.fileID == b.activeFileID {
			continue // 跳过当前活跃文件中的条目
		}

		value, err := b.Get(key)
		if err != nil {
			return err
		}

		// 写入数据
		valuePos, err := mergedFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		header := make([]byte, headerSize)
		binary.BigEndian.PutUint64(header[4:], uint64(record.timestamp))
		binary.BigEndian.PutUint32(header[12:], uint32(len(key)))
		binary.BigEndian.PutUint32(header[16:], uint32(len(value)))

		crc := crc32.ChecksumIEEE(append([]byte(key), value...))
		binary.BigEndian.PutUint32(header[:4], crc)

		if _, err := mergedFile.Write(header); err != nil {
			return err
		}
		if _, err := mergedFile.Write([]byte(key)); err != nil {
			return err
		}
		if _, err := mergedFile.Write(value); err != nil {
			return err
		}

		// 更新keydir
		et := entry{
			fileID:    mergedFileID,
			valueSize: int32(len(value)),
			valuePos:  valuePos + headerSize + int64(len(key)),
			timestamp: record.timestamp,
		}
		b.keydir[key] = et

		// 写入hint文件
		err = b.writeHintEntry(mergedHintFile, key, et)
		if err != nil {
			return err
		}
	}

	// 删除旧文件
	for _, file := range dataFiles {
		if filepath.Base(file) != fmt.Sprintf("%d.data", b.activeFileID) {
			os.Remove(file)
			os.Remove(strings.TrimSuffix(file, ".data") + ".hint")
		}
	}

	return nil
}
