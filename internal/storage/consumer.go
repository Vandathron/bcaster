package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/tysonmote/gommap"
	"io"
	"os"
	"sync"
)

var (
	offSize      = 8  // 8 bytes as offsetSize
	IdSize       = 35 // 35 bytes as ID size
	TopicSize    = 35 // 35 bytes as topic
	consumerSize = offSize + IdSize + TopicSize
)

type Consumer struct {
	file     *os.File
	mmap     gommap.MMap
	maxSize  uint32
	currSize uint32
	lock     sync.Mutex
	nextOff  uint32
	baseOff  uint32
}

func NewConsumer(fileName string, maxSize uint32, baseOff uint32) (*Consumer, error) {
	c := &Consumer{maxSize: maxSize, baseOff: baseOff}

	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	fileInf, err := file.Stat()
	if err != nil {
		return nil, err
	}

	c.file = file
	c.currSize = uint32(fileInf.Size())
	if c.currSize > c.maxSize {
		return nil, fmt.Errorf("current file size %d exceeds max size %d", c.currSize, c.maxSize)
	}

	c.nextOff = c.currSize/uint32(consumerSize) + baseOff
	if c.currSize == 0 { // truncate the empty file to allow for memory-mapping
		if err := c.file.Truncate(int64(consumerSize)); err != nil {
			return nil, err
		}
	}
	mmap, err := gommap.Map(file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)

	if err != nil {
		return nil, err
	}
	c.mmap = mmap
	return c, nil
}

func (c *Consumer) Append(id []byte, topic []byte, readOff uint64) (off uint32, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	err = c.mmap.Sync(gommap.MS_SYNC) // flush consumers info to storage before resizing/truncation memory map to append new consumer
	if err != nil {
		return 0, err
	}

	if len(id) > IdSize {
		return 0, fmt.Errorf("ID of length %v exceeds maximum size of %v", len(id), IdSize)
	}

	if len(topic) > TopicSize {
		return 0, fmt.Errorf("topic of size %v exceeds max size of %v", len(topic), TopicSize)
	}

	if c.isMaxed() {
		return 0, io.EOF
	}

	if err = c.makeSpaceForExtraConsumer(); err != nil {
		return 0, err
	}

	buf := make([]byte, consumerSize)
	copy(buf[:IdSize], id)
	copy(buf[IdSize:IdSize+TopicSize], topic)
	binary.BigEndian.PutUint64(buf[IdSize+TopicSize:], readOff)
	copy(c.mmap[c.currSize:c.currSize+uint32(consumerSize)], buf)

	if err = c.mmap.Sync(gommap.MS_SYNC); err != nil {
		return 0, err
	}
	off = c.nextOff
	c.currSize += uint32(consumerSize)
	c.nextOff++ // update next offset to write
	return off, nil
}

func (c *Consumer) WriteAt(off uint32, id []byte, topic []byte, readOff uint64) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(id) > IdSize {
		return fmt.Errorf("ID of length %v exceeds maximum size of %v", len(id), IdSize)
	}

	if len(topic) > TopicSize {
		return fmt.Errorf("topic of size %v exceeds max size of %v", len(topic), TopicSize)
	}

	if off >= c.nextOff {
		return fmt.Errorf("invalid offset %v. Last written offset: %v", off, c.nextOff-1)
	}

	buf := make([]byte, consumerSize)
	copy(buf[:IdSize], id)
	copy(buf[IdSize:IdSize+TopicSize], topic)
	binary.BigEndian.PutUint64(buf[IdSize+TopicSize:], readOff)
	startPos := off * uint32(consumerSize)
	copy(c.mmap[startPos:startPos+uint32(consumerSize)], buf)

	if err := c.mmap.Sync(gommap.MS_ASYNC); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) Read(off uint32, ignoreOff bool) (id []byte, topic []byte, readOff uint64, err error) {
	if off >= c.nextOff {
		return nil, nil, 0, io.EOF
	}

	startPos := off * uint32(consumerSize)
	consumer := c.mmap[startPos : startPos+uint32(consumerSize)]
	id = bytes.Trim(consumer[:IdSize], "\x00")                    // trim padded zero-bytes
	topic = bytes.Trim(consumer[IdSize:IdSize+TopicSize], "\x00") // trim padded zero-bytes
	readOff = binary.BigEndian.Uint64(consumer[IdSize+TopicSize:])

	// update nextOffset
	if !ignoreOff {
		binary.BigEndian.PutUint64(c.mmap[startPos+uint32(IdSize+TopicSize):], readOff+1)
	}

	return id, topic, readOff, nil
}

func (c *Consumer) Close() error {
	if err := c.mmap.UnsafeUnmap(); err != nil {
		return err
	}

	if err := os.Truncate(c.file.Name(), int64(c.currSize)); err != nil {
		return err
	}

	return c.file.Close()
}

func (c *Consumer) LatestCommitedOff() uint32 {
	return c.nextOff - 1
}

func (c *Consumer) isMaxed() bool {
	return c.currSize+uint32(consumerSize) > c.maxSize
}

func (c *Consumer) makeSpaceForExtraConsumer() error {
	if c.isMaxed() {
		return io.EOF
	}
	err := os.Truncate(c.file.Name(), int64(c.currSize+uint32(consumerSize)))
	if err != nil {
		return err
	}

	if c.mmap != nil {
		err = c.mmap.UnsafeUnmap() // syncs changes under the hood
		if err != nil {
			return err
		}
	}

	c.mmap, err = gommap.Map(c.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return err
	}
	return nil
}
