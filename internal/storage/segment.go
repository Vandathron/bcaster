package storage

import (
	"fmt"
	"github.com/vandathron/bcaster/internal/cfg"
	"io"
	"path/filepath"
)

type Segment struct {
	index      *MsgIdx
	msgFile    *msgFile
	cfg        cfg.Segment
	nextOffset uint64
	name       string
}

func NewSegment(dir string, config cfg.Segment) (*Segment, error) {
	s := &Segment{
		cfg:  config,
		name: formatName(config.StartOffset, dir, ""),
	}
	var err error
	s.index, err = NewIndex(formatName(s.cfg.StartOffset, dir, ".index"), cfg.Index{
		MaxSizeByte: s.cfg.MaxIdxSizeByte,
	})
	if err != nil {
		return nil, err
	}

	s.msgFile, err = NewMsgFile(formatName(s.cfg.StartOffset, dir, ".message"), s.cfg.MaxMsgSizeByte)
	if err != nil {
		_ = s.index.Close()
		return nil, err
	}

	lastOffset, _, err := s.index.LastEntry()

	if err == io.EOF { // indicates an empty index file. Next offset should be base offset
		s.nextOffset = s.cfg.StartOffset
	} else {
		s.nextOffset = lastOffset + 1 // Set future entry write offset
	}

	return s, nil
}

func (s *Segment) Append(msg []byte) (uint64, error) {
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

func (s *Segment) Read(offset uint64) ([]byte, error) {
	if offset > s.nextOffset {
		return nil, io.EOF // Offset not within segment
	}

	pos, err := s.index.Read(offset - s.cfg.StartOffset)
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

func (s *Segment) LatestCommittedOff() uint64 {
	return s.nextOffset - 1
}

func formatName(startOffset uint64, dir, ext string) string {
	return filepath.Join(dir, fmt.Sprintf("%d%s", startOffset, ext))
}

func (s *Segment) IsFull() bool {
	return s.index.IsMaxedOut() || s.msgFile.IsMaxedOut()
}
