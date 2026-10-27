package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Record represents a single message stored in the log.
type Record struct {
	Offset uint64
	Value  []byte
}

// Log represents an append-only log for a single partition.
type Log struct {
	mu         sync.Mutex
	dir        string
	file       *os.File
	nextOffset uint64
	// index maps offset -> byte position in the file for O(1) lookups
	index map[uint64]int64
}

// NewLog initializes or opens a log directory
func NewLog(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	logPath := filepath.Join(dir, "partition.log")
	file, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	l := &Log{dir: dir, file: file, index: make(map[uint64]int64)}

	// Recover index and nextOffset from existing file if restarting
	if err := l.recover(); err != nil {
		return nil, err
	}

	return l, nil
}

// Append writes a new record to the end of the log and returns its assigned offset.
func (l *Log) Append(value []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset := l.nextOffset

	// Get current file position before writing
	pos, err := l.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	// Format: [Offset (8 bytes)][Length (4 bytes)][Value (N bytes)]
	length := uint32(len(value))
	buf := make([]byte, 12+length)
	binary.BigEndian.PutUint64(buf[0:8], offset)
	binary.BigEndian.PutUint32(buf[8:12], length)
	copy(buf[12:], value)

	if _, err := l.file.Write(buf); err != nil {
		return 0, err
	}

	// Update in-memory index and increment offset
	l.index[offset] = pos
	l.nextOffset++

	return offset, nil
}

// Read retrieves a record by it's offset.
func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	pos, exists := l.index[offset]
	if !exists {
		return nil, fmt.Errorf("offset %d not found", offset)
	}

	// Seek to the record position
	if _, err := l.file.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}

	// Read offset (8 bytes) + length (4 bytes)
	header := make([]byte, 12)
	if _, err := io.ReadFull(l.file, header); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header[8:12])
	value := make([]byte, length)
	if _, err := io.ReadFull(l.file, value); err != nil {
		return nil, err
	}

	return value, nil
}

// recover scans the log file on startup to rebuild the index.
func (l *Log) recover() error {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var pos int64 = 0
	for {
		header := make([]byte, 12)
		_, err := io.ReadFull(l.file, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return err
		}

		offset := binary.BigEndian.Uint64(header[0:8])
		length := binary.BigEndian.Uint32(header[8:12])

		// Skip over the value payload
		if _, err := l.file.Seek(int64(length), io.SeekCurrent); err != nil {
			return err
		}

		l.index[offset] = pos
		l.nextOffset = offset + 1

		// Update expected next position
		pos += int64(12 + length)
	}

	return nil
}

// NextOffset return the next offset that will be assigned in this log.
// This gives us an O(1) way to know what offset we need to fetch next.
func (l *Log) NextOffset() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextOffset
}

// Close safely closes the log file handle.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
