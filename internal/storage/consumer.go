package storage

import (
	"encoding/binary"
	"fmt"
	"github.com/tysonmote/gommap"
	"io"
	"os"
	"sync"
)

var (
	offSize      = 8  // 8 bytes as offsetSize
	idSize       = 30 // 30 bytes as ID size
	posSize      = 4  // 4 bytes to store consumer position in file
	consumerSize = offSize + idSize + posSize
)

type config struct {
	maxSize uint32
}

type Consumer struct {
	file *os.File
	mmap gommap.MMap
	config
	currSize uint32
	lock     sync.Mutex
	nextPos  uint32
}

func NewConsumer(fileName string, cfg config) (*Consumer, error) {
	c := &Consumer{config: cfg}

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

	c.nextPos = c.currSize
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

func (c *Consumer) Append(id []byte, nextOff uint64) (pos uint32, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	err = c.mmap.Sync(gommap.MS_SYNC) // flush consumers info to storage before resizing/truncation memory map to append new consumer
	if err != nil {
		return 0, err
	}

	if len(id) > idSize {
		return 0, fmt.Errorf("ID of length %v exceeds maximum size of %v", len(id), idSize)
	}

	if !c.isSpaceAvailable() {
		return 0, io.EOF
	}

	if err = c.makeSpaceForExtraConsumer(); err != nil {
		return 0, err
	}

	buf := make([]byte, consumerSize)
	copy(buf[:idSize], id)
	binary.BigEndian.PutUint64(buf[idSize:idSize+offSize], nextOff)
	binary.BigEndian.PutUint32(buf[idSize+offSize:], c.nextPos)
	copy(c.mmap[c.nextPos:c.nextPos+uint32(consumerSize)], buf)

	if err = c.mmap.Sync(gommap.MS_SYNC); err != nil {
		return 0, err
	}

	pos = c.currSize
	c.currSize += uint32(consumerSize)
	c.nextPos = c.currSize
	return pos, nil
}

func (c *Consumer) Read(pos uint32) (id []byte, nextOff uint64, err error) {
	if pos >= c.nextPos {
		return nil, 0, fmt.Errorf("invalid position %v. Last write pos: %v", pos, c.nextPos)
	}

	id = c.mmap[pos : pos+uint32(idSize)]
	nextOff = binary.BigEndian.Uint64(c.mmap[pos+uint32(idSize) : pos+uint32(idSize+offSize)])
	// update nextOffset
	binary.BigEndian.PutUint64(c.mmap[pos+uint32(idSize+offSize):pos+uint32(consumerSize)], nextOff+1)

	return id, nextOff, nil
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

func (c *Consumer) isSpaceAvailable() bool {
	return c.currSize <= c.maxSize
}

func (c *Consumer) makeSpaceForExtraConsumer() error {
	if !c.isSpaceAvailable() {
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
