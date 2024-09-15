package storage

type SegmentConfig struct {
	AllowedIndexSize int32
	AllowedStoreSize int32
}

type Segment struct {
	index   *msgIdx
	msgFile *msgFile
	SegmentConfig
}

func NewSegment(maxIdxSizeByte, maxMsgFileSizeByte uint32) (*Segment, error) {
	return &Segment{}, nil
}

func (s *Segment) Append(msg []byte) {

}

func (s *Segment) Read() {

}

func (s *Segment) Close() {

}
