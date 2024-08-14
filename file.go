package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func (b *Bitcask) loadHintFiles() error {
	hintFiles, err := filepath.Glob(filepath.Join(b.directory, "*.hint"))
	if err != nil {
		return err
	}

	for _, hintFile := range hintFiles {
		file, err := os.Open(hintFile)
		if err != nil {
			return err
		}
		defer file.Close()

		reader := bufio.NewReader(file)
		for {
			record := make([]byte, 28) // key size (4) + value size (4) + value pos (8) + timestamp (8) + file id (4)
			_, err := io.ReadFull(reader, record)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			keySize := int(binary.BigEndian.Uint32(record[:4]))
			valueSize := int64(binary.BigEndian.Uint32(record[4:8]))
			valuePos := int64(binary.BigEndian.Uint64(record[8:16]))
			timestamp := int64(binary.BigEndian.Uint64(record[16:24]))
			fileID := int(binary.BigEndian.Uint32(record[24:28]))

			key := make([]byte, keySize)
			if _, err := io.ReadFull(reader, key); err != nil {
				return err
			}

			b.keydir[string(key)] = entry{
				fileID:    fileID,
				valueSize: valueSize,
				valuePos:  valuePos,
				timestamp: timestamp,
			}
		}
	}

	return nil
}

func (b *Bitcask) openNewActiveFile() error {
	if b.activeFile != nil {
		b.activeFile.Close()
	}

	b.activeFileID = int(time.Now().UnixNano())
	filename := b.getDataFilePath(b.activeFileID)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	b.activeFile = file

	// 创建对应的hint文件
	hintFilename := b.getHintFilePath(b.activeFileID)
	hintFile, err := os.Create(hintFilename)
	if err != nil {
		return err
	}
	hintFile.Close()

	return nil
}

func (b *Bitcask) getDataFilePath(fileID int) string {
	return filepath.Join(b.directory, fmt.Sprintf("%d.data", fileID))
}

func (b *Bitcask) getHintFilePath(fileID int) string {
	return filepath.Join(b.directory, fmt.Sprintf("%d.hint", fileID))
}

func (b *Bitcask) copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func (b *Bitcask) mmapFile(file *os.File) (*MmapedFile, error) {
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := fi.Size()
	if size == 0 {
		return &MmapedFile{data: []byte{}, file: file}, nil
	}

	data, err := unix.Mmap(int(file.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &MmapedFile{data: data, file: file}, nil
}

func (b *Bitcask) unmmapFile(mf *MmapedFile) error {
	if mf.data != nil {
		if err := unix.Munmap(mf.data); err != nil {
			return err
		}
		mf.data = nil
	}
	return mf.file.Close()
}

func (b *Bitcask) loadExistingFiles() error {
	files, err := filepath.Glob(filepath.Join(b.directory, "*.data"))
	if err != nil {
		return fmt.Errorf("failed to glob data files: %w", err)
	}

	for _, file := range files {
		fileID, err := strconv.Atoi(strings.TrimSuffix(filepath.Base(file), ".data"))
		if err != nil {
			return fmt.Errorf("invalid file name: %s", file)
		}

		if fileID > b.activeFileID {
			b.activeFileID = fileID
		}

		if err := b.loadHintFile(fileID); err != nil {
			return fmt.Errorf("failed to load hint file for %d: %w", fileID, err)
		}
	}

	return nil
}

func (b *Bitcask) loadHintFile(fileID int) error {
	hintPath := b.getHintFilePath(fileID)
	file, err := os.Open(hintPath)
	if err != nil {
		if os.IsNotExist(err) {
			// 如果提示文件不存在，我们需要从数据文件重建它
			return b.rebuildHintFile(fileID)
		}
		return fmt.Errorf("failed to open hint file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		record := make([]byte, 28) // key size (4) + value size (4) + value pos (8) + timestamp (8) + file id (4)
		_, err := io.ReadFull(reader, record)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read hint entry: %w", err)
		}

		keySize := int(binary.BigEndian.Uint32(record[:4]))
		valueSize := int64(binary.BigEndian.Uint32(record[4:8]))
		valuePos := int64(binary.BigEndian.Uint64(record[8:16]))
		timestamp := int64(binary.BigEndian.Uint64(record[16:24]))
		entryFileID := int(binary.BigEndian.Uint32(record[24:28]))

		key := make([]byte, keySize)
		if _, err := io.ReadFull(reader, key); err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}

		b.keydir[string(key)] = entry{
			fileID:    entryFileID,
			valueSize: valueSize,
			valuePos:  valuePos,
			timestamp: timestamp,
		}
	}

	return nil
}

func (b *Bitcask) rebuildHintFile(fileID int) error {
	dataPath := b.getDataFilePath(fileID)
	file, err := os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	hintPath := b.getHintFilePath(fileID)
	hintFile, err := os.Create(hintPath)
	if err != nil {
		return fmt.Errorf("failed to create hint file: %w", err)
	}
	defer hintFile.Close()

	reader := bufio.NewReader(file)
	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(reader, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read header: %w", err)
		}

		timestamp := int64(binary.BigEndian.Uint64(header[4:]))
		keySize := int(binary.BigEndian.Uint32(header[12:]))
		valueSize := int64(binary.BigEndian.Uint32(header[16:]))

		key := make([]byte, keySize)
		if _, err := io.ReadFull(reader, key); err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}

		valuePos, _ := file.Seek(0, io.SeekCurrent)

		// 写入提示文件
		hintEntry := make([]byte, 28)
		binary.BigEndian.PutUint32(hintEntry[:4], uint32(keySize))
		binary.BigEndian.PutUint32(hintEntry[4:8], uint32(valueSize))
		binary.BigEndian.PutUint64(hintEntry[8:16], uint64(valuePos))
		binary.BigEndian.PutUint64(hintEntry[16:24], uint64(timestamp))
		binary.BigEndian.PutUint32(hintEntry[24:28], uint32(fileID))

		if _, err := hintFile.Write(hintEntry); err != nil {
			return fmt.Errorf("failed to write hint entry: %w", err)
		}
		if _, err := hintFile.Write(key); err != nil {
			return fmt.Errorf("failed to write key to hint file: %w", err)
		}

		// 更新keydir
		b.keydir[string(key)] = entry{
			fileID:    fileID,
			valueSize: valueSize,
			valuePos:  valuePos,
			timestamp: timestamp,
		}

		// 跳过值
		if _, err := file.Seek(valueSize, io.SeekCurrent); err != nil {
			return fmt.Errorf("failed to seek past value: %w", err)
		}
	}

	return nil
}

func (b *Bitcask) openActiveFile(fileID int) error {
	filename := b.getDataFilePath(fileID)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open active file: %w", err)
	}
	b.activeFile = file
	b.activeFileID = fileID
	return nil
}

func (b *Bitcask) updateMmap(fileID int) error {
	mf, ok := b.mmapedFiles[fileID]
	if !ok {
		// 如果内存映射不存在，创建一个新的
		file, err := os.OpenFile(b.getDataFilePath(fileID), os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed to open data file for mmap: %w", err)
		}
		mf, err = b.mmapFile(file)
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to mmap file: %w", err)
		}
		b.mmapedFiles[fileID] = mf
		return nil
	}

	// 获取文件当前大小
	fileInfo, err := mf.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// 如果文件大小没有变化，不需要更新
	if fileInfo.Size() == int64(len(mf.data)) {
		return nil
	}

	// 解除旧的内存映射
	if err := unix.Munmap(mf.data); err != nil {
		return fmt.Errorf("failed to unmap old data: %w", err)
	}

	// 创建新的内存映射
	data, err := unix.Mmap(int(mf.file.Fd()), 0, int(fileInfo.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap file: %w", err)
	}

	// 更新内存映射
	mf.data = data
	b.mmapedFiles[fileID] = mf

	return nil
}

func (b *Bitcask) getActiveFileSize() int64 {
	if b.activeFile == nil {
		return 0
	}
	info, err := b.activeFile.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}
