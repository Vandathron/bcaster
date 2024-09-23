package storage

import (
	"fmt"
	"io"
	"path/filepath"
)

type segmentConfig struct {
	maxIdxSizeByte uint64
	maxMsgSizeByte uint64
	startOffset    uint32
}

type Segment struct {
	index      *msgIdx
	msgFile    *msgFile
	c          segmentConfig
	nextOffset uint32
	name       string
}

func NewSegment(dir string, config segmentConfig) (*Segment, error) {
	s := &Segment{
		c:    config,
		name: formatName(config.startOffset, dir, ""),
	}
	var err error
	s.index, err = NewIndex(formatName(s.c.startOffset, dir, ".index"), s.c.maxIdxSizeByte)
	if err != nil {
		return nil, err
	}

	s.msgFile, err = NewMsgFile(formatName(s.c.startOffset, dir, ".message"), s.c.maxMsgSizeByte)
	if err != nil {
		_ = s.index.Close()
		return nil, err
	}

	lastOffset, _, err := s.index.LastEntry()

	if err == io.EOF { // indicates an empty index file. Next offset should be 0
		s.nextOffset = lastOffset
	} else {
		s.nextOffset = lastOffset + 1 // Set future entry write offset
	}

	return s, nil
}

func (s *Segment) Append(msg []byte) (uint32, error) {
	pos, err := s.msgFile.Append(msg)
	if err != nil {
		return 0, err
	}

	err = s.index.Append(s.nextOffset, pos)
	if err != nil {
		return 0, err
	}

	s.nextOffset++
	return s.nextOffset - 1, nil
}

func (s *Segment) Read(offset uint32) ([]byte, error) {
	if offset > s.nextOffset {
		return nil, io.EOF // Offset not within segment
	}

	pos, err := s.index.Read(offset)
	if err != nil {
		return nil, err
	}

	msg, err := s.msgFile.Read(pos)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (s *Segment) Close() error {
	err := s.msgFile.Close()
	if err != nil {
		return err
	}
	return s.index.Close()
}

func formatName(startOffset uint32, dir, ext string) string {
	return filepath.Join(dir, fmt.Sprintf("%d%s", startOffset, ext))
}

func (s *Segment) IsFull() bool {
	return s.index.IsMaxedOut() || s.msgFile.IsMaxedOut()
}
