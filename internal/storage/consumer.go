package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/tysonmote/gommap"
	"github.com/vandathron/bcaster/internal/cfg"
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

type Consumer struct {
	file     *os.File
	mmap     gommap.MMap
	cfg      cfg.Consumer
	currSize uint32
	lock     sync.Mutex
	nextPos  uint32
}

func NewConsumer(fileName string, cfg cfg.Consumer) (*Consumer, error) {
	c := &Consumer{cfg: cfg}

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
	if c.currSize > c.cfg.MaxSize {
		return nil, fmt.Errorf("current file size %d exceeds max size %d", c.currSize, c.cfg.MaxSize)
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

func (c *Consumer) WriteAt(pos uint32, id []byte, nextOff uint64) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(id) > idSize {
		return fmt.Errorf("ID of length %v exceeds maximum size of %v", len(id), idSize)
	}

	if pos >= c.nextPos {
		return fmt.Errorf("invalid position %v. Last write pos: %v", pos, c.nextPos-1)
	}

	buf := make([]byte, consumerSize)
	copy(buf[:idSize], id)
	binary.BigEndian.PutUint64(buf[idSize:idSize+offSize], nextOff)
	copy(c.mmap[pos:pos+uint32(idSize+offSize)], buf[:idSize+offSize])

	if err := c.mmap.Sync(gommap.MS_ASYNC); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) Read(pos uint32, incOffset bool) (id []byte, off uint64, err error) {
	if pos >= c.nextPos {
		return nil, 0, fmt.Errorf("invalid position %v. Last write pos: %v", pos, c.nextPos)
	}

	id = bytes.Trim(c.mmap[pos:pos+uint32(idSize)], "\x00") // trim padded zero-bytes
	off = binary.BigEndian.Uint64(c.mmap[pos+uint32(idSize) : pos+uint32(idSize+offSize)])
	// update nextOffset
	if incOffset {
		binary.BigEndian.PutUint64(c.mmap[pos+uint32(idSize):pos+uint32(idSize+offSize)], off+1)
	}
	return id, off, nil
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
	return c.currSize <= c.cfg.MaxSize
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
