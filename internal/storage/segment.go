package storage

import (
	"fmt"
	"io"
	"path/filepath"
)

type config struct {
	allowedIndexSize uint64
	allowedMsgSize   uint64
	startOffset      uint32
}

type Segment struct {
	index   *msgIdx
	msgFile *msgFile
	config
	nextOffset uint32
}

func NewSegment(dir string, config config) (*Segment, error) {
	s := &Segment{
		config: config,
	}
	var err error
	s.index, err = NewIndex(filepath.Join(dir, fmt.Sprintf("%d%s", s.startOffset, ".index")), s.allowedIndexSize)
	if err != nil {
		return nil, err
	}

	s.msgFile, err = NewMsgFile(filepath.Join(dir, fmt.Sprintf("%d%s", s.startOffset, ".message")), s.allowedMsgSize)
	if err != nil {
		_ = s.index.Close()
		return nil, err
	}

	lastOffset, _, err := s.index.LastEntry()

	if err == io.EOF {
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

func (s *Segment) IsFull() bool {
	return s.index.IsMaxedOut() || s.msgFile.IsMaxedOut()
}
