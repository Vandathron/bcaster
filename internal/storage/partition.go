package storage

import (
	"fmt"
	"github.com/vandathron/bcaster/internal/cfg"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Partition struct {
	topic           string
	segments        []*Segment
	writableSegment *Segment
	cfg             cfg.Partition
}

func NewPartition(topic string, c cfg.Partition) (*Partition, error) {
	partitionDir := filepath.Join(c.BaseDir, fmt.Sprintf("part_%s", topic))
	if err := os.MkdirAll(partitionDir, 0750); err != nil {
		return nil, err
	}

	segments, err := os.ReadDir(partitionDir) // Usually directory may contain messages & index, segment is just a logical name
	if err != nil {
		return nil, err
	}
	p := &Partition{
		cfg:   c,
		topic: topic,
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i].Name() < segments[j].Name() })

	for i := 0; i < len(segments); i = i + 2 { // each segment has msg file and index file with same name. Makes sense to only get one of each.
		segment := segments[i]
		segName := strings.TrimSuffix(segment.Name(), path.Ext(segment.Name()))
		if segName == "" {
			return nil, fmt.Errorf("invalid segment name: %s", segment.Name())
		}
		startOffset, err := strconv.Atoi(segName)
		if err != nil {
			return nil, fmt.Errorf("invalid segment name. should be an integer: %s", segment.Name())
		}

		p.cfg.Segment.StartOffset = uint32(startOffset)

		s, err := NewSegment(partitionDir, p.cfg.Segment)

		if err != nil {
			return nil, err
		}

		p.segments = append(p.segments, s)
		p.writableSegment = s // Will assign last segment eventually as writable segment
	}

	if len(segments) == 0 { // indicates an empty partition
		p.cfg.Segment.StartOffset = uint32(0)
		s, err := NewSegment(partitionDir, p.cfg.Segment)

		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, s)
		p.writableSegment = s
	}

	return p, nil
}

func (p *Partition) Append(message []byte) (uint32, error) {
	off, err := p.writableSegment.Append(message)
	if err != nil {
		if err == io.EOF { // indicates a full segment and should create a new segment, then add/update writable segment
			p.cfg.Segment.StartOffset = p.writableSegment.nextOffset

			s, err := NewSegment(p.Name(), p.cfg.Segment)

			if err != nil {
				return 0, err
			}

			p.segments = append(p.segments, s)
			p.writableSegment = s
			return p.Append(message)
		}
		return 0, err
	}

	return off, nil
}

func (p *Partition) Read(offset uint32) (message []byte, err error) {
	segment := p.getOffsetSegment(offset)
	if segment == nil {
		return nil, fmt.Errorf("invalid offset: %d", offset)
	}

	return segment.Read(offset)
}

func (p *Partition) getOffsetSegment(offset uint32) *Segment {
	for _, segment := range p.segments {
		if offset >= segment.cfg.StartOffset && offset < segment.nextOffset {
			return segment
		}
	}
	return nil
}

func (p *Partition) Name() string {
	return filepath.Join(p.cfg.BaseDir, fmt.Sprintf("part_%s", p.topic))
}

func (p *Partition) Close() error {
	for _, s := range p.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}
