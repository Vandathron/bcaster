package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

const (
	msgLenWidth = 8 // 8 bytes to hold the value of the message length
)

type LogFileConfig struct {
}

type msgFile struct {
	lck         sync.Mutex
	currSize    uint64
	file        *os.File
	tempStorage *bufio.Writer
	maxFileSize uint64
}

func NewMsgFile(filename string, maxFileSize uint64) (*msgFile, error) {
	logFile := &msgFile{maxFileSize: maxFileSize}

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

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

func (m *msgFile) Append(data []byte) (pos uint64, err error) {
	m.lck.Lock()
	defer m.lck.Unlock()
	dataLen := len(data)
	entryWidth := dataLen + msgLenWidth
	pos = m.currSize // new entry pos

	if m.currSize+uint64(entryWidth) > m.maxFileSize {
		return pos, io.EOF
	}

	// Write data length (8 bytes) to temp storage
	err = binary.Write(m.tempStorage, binary.BigEndian, uint64(dataLen))
	if err != nil {
		return pos, err
	}

	_, err = m.tempStorage.Write(data)
	if err != nil {
		return pos, err
	}

	m.currSize += uint64(entryWidth)
	return pos, err
}

func (m *msgFile) Read(pos uint64) ([]byte, error) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if pos > m.currSize {
		return nil, errors.New("unexpected behaviour: pos should not exceed current size of log file")
	}
	// TODO: improve how flush is done. no flushing after every read.
	err := m.tempStorage.Flush() // Write to permanent storage

	if err != nil {
		return nil, err
	}

	msgSizeBytes := make([]byte, msgLenWidth)
	_, err = m.file.ReadAt(msgSizeBytes, int64(pos))
	if err != nil {
		return nil, err
	}
	msgSizeVal := binary.BigEndian.Uint64(msgSizeBytes)
	msg := make([]byte, msgSizeVal)

	_, err = m.file.ReadAt(msg, int64(pos+msgLenWidth))
	if err != nil {
		return nil, err
	}

	return msg, err
}

func (m *msgFile) CurrentSize() uint64 {
	return m.currSize
}

func (m *msgFile) Close() error {
	m.lck.Lock()
	defer m.lck.Unlock()
	err := m.tempStorage.Flush()
	if err != nil {
		return err
	}
	err = m.file.Close()
	return err
}

func (m *msgFile) IsMaxedOut() bool {
	return m.currSize >= m.maxFileSize
}
