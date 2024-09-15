package storage

import (
	"encoding/binary"
	"errors"
	"github.com/tysonmote/gommap"
	"io"
	"os"
)

const (
	offsetWidth     = 4                         // offset value in index file should occupy 4 bytes in space
	msgPosWidth     = 8                         // Message pos value in index file should occupy 8 bytes in space
	indexEntryWidth = offsetWidth + msgPosWidth // Full index entry by logic should occupy 12 bytes
)

type msgIdx struct {
	file     *os.File
	mmap     gommap.MMap
	currSize uint64
	maxSize  uint64
}

func NewIndex(fileName string, maxSize uint64) (*msgIdx, error) {
	idx := &msgIdx{maxSize: maxSize}
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)

	if err != nil {
		return nil, err
	}

	// Get actual/current file size prior truncate()
	fInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	idx.currSize = uint64(fInfo.Size())

	// Attempts to truncate file size to max size specified as mmap attempts to map entire file to virtual address
	err = f.Truncate(int64(maxSize))
	if err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	idx.file = f
	return idx, nil
}

func (i *msgIdx) Append(off uint32, pos uint64) error {
	if indexEntryWidth+i.currSize > i.maxSize {
		return io.EOF
	}

	binary.BigEndian.PutUint32(i.mmap[i.currSize:i.currSize+offsetWidth], off)
	binary.BigEndian.PutUint64(i.mmap[i.currSize+offsetWidth:i.currSize+indexEntryWidth], pos)
	i.currSize += indexEntryWidth

	err := i.mmap.Sync(gommap.MS_SYNC)
	return err
}

func (i *msgIdx) Read(off int32) (uint64, error) {
	startPos := uint32(0)
	if off < -1 {
		return 0, errors.New("offset out of range")
	}

	if off == -1 { // Pick last file entry
		startPos = uint32(i.currSize/indexEntryWidth) - 1
	} else {
		startPos = uint32(off * indexEntryWidth)
	}

	startPos += offsetWidth
	endPos := startPos + indexEntryWidth
	entryPos := binary.BigEndian.Uint64(i.mmap[startPos:endPos]) // Fetch entry position from index
	return entryPos, nil
}

func (i *msgIdx) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil { // synchronously flush to file
		return err
	}
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return err
	}
	return i.file.Close()
}
