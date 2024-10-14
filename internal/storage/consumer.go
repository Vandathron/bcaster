package storage

import (
	"fmt"
	"github.com/tysonmote/gommap"
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
