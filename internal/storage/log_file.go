package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"os"
	"sync"
)

const (
	msgLenWidth = 8 // 8 bytes to hold the value of the message length
)

type LogFileConfig struct {
}

type LogFile struct {
	lck         sync.Mutex
	currSize    uint64
	file        *os.File
	tempStorage *bufio.Writer
	maxFileSize uint64
}

func NewLogFile(filename string, maxFileSize uint64) (*LogFile, error) {
	logFile := &LogFile{maxFileSize: maxFileSize}

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	logFile.file = file
	logFile.currSize = uint64(fileInfo.Size())
	logFile.tempStorage = bufio.NewWriter(logFile.file)

	return logFile, nil
}

func (l *LogFile) Append(data []byte) (pos uint64, err error) {
	l.lck.Lock()
	defer l.lck.Unlock()
	dataLen := len(data)
	entryWidth := dataLen + msgLenWidth
	pos = l.currSize // new entry pos

	if l.currSize+uint64(entryWidth) > l.maxFileSize {
		return pos, errors.New("unexpected behaviour: log file should not be active")
	}

	// Write data length (8 bytes) to temp storage
	err = binary.Write(l.tempStorage, binary.BigEndian, uint64(dataLen))
	if err != nil {
		return pos, err
	}

	_, err = l.tempStorage.Write(data)
	if err != nil {
		return pos, err
	}

	l.currSize += uint64(entryWidth)
	return pos, err
}

func (l *LogFile) Read(pos uint64) ([]byte, error) {
	l.lck.Lock()
	defer l.lck.Unlock()

	if pos > l.currSize {
		return nil, errors.New("unexpected behaviour: pos should not exceed current size of log file")
	}

	err := l.tempStorage.Flush() // Write to permanent storage

	if err != nil {
		return nil, err
	}

	msgSizeBytes := make([]byte, msgLenWidth)
	_, err = l.file.ReadAt(msgSizeBytes, int64(pos))
	if err != nil {
		return nil, err
	}
	msgSizeVal := binary.BigEndian.Uint64(msgSizeBytes)
	msg := make([]byte, msgSizeVal)

	_, err = l.file.ReadAt(msg, int64(pos+msgLenWidth))
	if err != nil {
		return nil, err
	}

	return msg, err
}

func (l *LogFile) CurrentSize() uint64 {
	return l.currSize
}

func (l *LogFile) Close() error {
	l.lck.Lock()
	defer l.lck.Unlock()
	err := l.tempStorage.Flush()
	if err != nil {
		return err
	}
	err = l.file.Close()
	return err
}
