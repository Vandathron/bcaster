package storage

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type PartitionConfig struct {
	dir string
}

type Partition struct {
	topic           string
	segments        []*Segment
	writableSegment *Segment
	config          PartitionConfig
}

func NewPartition(topic string, c PartitionConfig) (*Partition, error) {
	partitionDir := filepath.Join(c.dir, fmt.Sprintf("part_%s", topic))
	if err := os.MkdirAll(partitionDir, 0750); err != nil {
		return nil, err
	}

	segments, err := os.ReadDir(partitionDir) // Usually directory may contain messages & index, segment is just a logical name
	if err != nil {
		return nil, err
	}
	p := &Partition{
		config: c,
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i].Name() < segments[j].Name() })

	for _, segment := range segments {
		if segment.IsDir() {
			continue
		}

		segName := strings.TrimSuffix(segment.Name(), path.Ext(segment.Name()))
		if segName == "" {
			return nil, fmt.Errorf("invalid segment name: %s", segment.Name())
		}
		startOffset, err := strconv.Atoi(segName)
		if err != nil {
			return nil, fmt.Errorf("invalid segment name. should be integer: %s", segment.Name())
		}

		s, err := NewSegment(filepath.Join(partitionDir, segName), config{
			allowedIndexSize: 800,
			allowedMsgSize:   1024,
			startOffset:      uint32(startOffset),
		})

		if err != nil {
			return nil, err
		}

		p.segments = append(p.segments, s)
	}

	if len(segments) == 0 { // indicates an empty partition
		s, err := NewSegment(filepath.Join(partitionDir, "0"), config{
			allowedIndexSize: 800,
			allowedMsgSize:   1024,
			startOffset:      uint32(0),
		})

		if err != nil {
			return nil, err
		}
		p.segments = append(p.segments, s)
		p.writableSegment = s
	}

	return nil, nil
}

func (p *Partition) Append(message []byte) (uint32, error) {
	off, err := p.writableSegment.Append(message)
	if err != nil {
		if err == io.EOF { // indicates a full segment and should add/update writable segment
			s, err := NewSegment(filepath.Join(p.config.dir, fmt.Sprintf("%d", p.writableSegment.nextOffset)), config{
				allowedIndexSize: 800,
				allowedMsgSize:   1024,
				startOffset:      p.writableSegment.nextOffset,
			})

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

type StoreConfig struct {
	AutoCreateTopic bool
}

type Store struct {
	topicMap map[string]*Partition
}
