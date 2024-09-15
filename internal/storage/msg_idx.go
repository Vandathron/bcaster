package storage

import (
	"encoding/binary"
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

func (i *msgIdx) Read(off uint32) (uint64, error) {
	entryStartPos := off * indexEntryWidth

	if uint64(entryStartPos) > i.currSize+indexEntryWidth { // Check offset is within index entries
		return 0, io.EOF
	}

	entryEndPos := entryStartPos + indexEntryWidth
	entryPos := binary.BigEndian.Uint64(i.mmap[entryStartPos+offsetWidth : entryEndPos]) // Fetch entry position from index
	return entryPos, nil
}

func (i *msgIdx) LastEntry() (off uint32, pos uint64, err error) {
	if indexEntryWidth > i.currSize { // Should have at least one entry
		err = io.EOF
		return
	}

	startPos := i.currSize - indexEntryWidth

	off = binary.BigEndian.Uint32(i.mmap[startPos : startPos+offsetWidth])
	pos = binary.BigEndian.Uint64(i.mmap[startPos+offsetWidth : startPos+indexEntryWidth])

	return off, pos, nil
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

func (i *msgIdx) IsMaxedOut() bool {
	return i.currSize >= i.maxSize
}
