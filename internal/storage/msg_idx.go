package storage

import (
	"encoding/binary"
	"github.com/tysonmote/gommap"
	"github.com/vandathron/bcaster/internal/cfg"
	"io"
	"os"
)

const (
	offsetWidth     = 8                         // offset value in index file should occupy 8 bytes in space
	msgPosWidth     = 8                         // Message pos value in index file should occupy 8 bytes in space
	indexEntryWidth = offsetWidth + msgPosWidth // Full index entry by logic should occupy 16 bytes
)

type MsgIdx struct {
	file     *os.File
	mmap     gommap.MMap
	currSize uint64
	cfg      cfg.Index
}

func NewIndex(fileName string, cfg cfg.Index) (*MsgIdx, error) {
	idx := &MsgIdx{cfg: cfg}
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
	err = f.Truncate(int64(idx.cfg.MaxSizeByte))
	if err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	idx.file = f
	return idx, nil
}

func (i *MsgIdx) Append(off uint64, pos uint64) error {
	if indexEntryWidth+i.currSize > i.cfg.MaxSizeByte {
		return io.EOF
	}

	binary.BigEndian.PutUint64(i.mmap[i.currSize:i.currSize+offsetWidth], off)
	binary.BigEndian.PutUint64(i.mmap[i.currSize+offsetWidth:i.currSize+indexEntryWidth], pos)
	i.currSize += indexEntryWidth

	err := i.mmap.Sync(gommap.MS_SYNC)
	return err
}

func (i *MsgIdx) Read(off uint64) (uint64, error) {
	entryStartPos := off * indexEntryWidth
	entryEndPos := entryStartPos + indexEntryWidth

	if entryEndPos > i.currSize { // Check offset is within index entries
		return 0, io.EOF
	}

	entryPos := binary.BigEndian.Uint64(i.mmap[entryStartPos+offsetWidth : entryEndPos]) // Fetch entry position from index
	return entryPos, nil
}

func (i *MsgIdx) LastEntry() (off uint64, pos uint64, err error) {
	if indexEntryWidth > i.currSize { // Should have at least one entry
		err = io.EOF
		return
	}

	startPos := i.currSize - indexEntryWidth

	off = binary.BigEndian.Uint64(i.mmap[startPos : startPos+offsetWidth])
	pos = binary.BigEndian.Uint64(i.mmap[startPos+offsetWidth : startPos+indexEntryWidth])

	return off, pos, nil
}

func (i *MsgIdx) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil { // synchronously flush to file
		return err
	}
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.currSize)); err != nil {
		return err
	}

	return i.file.Close()
}

func (i *MsgIdx) Discard() error {
	if err := i.Close(); err != nil {
		return err
	}
	return os.Remove(i.file.Name())
}

func (i *MsgIdx) IsMaxedOut() bool {
	return i.currSize+indexEntryWidth >= i.cfg.MaxSizeByte
}
