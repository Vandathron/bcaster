package model

type Consumer struct {
	ID         string
	Topic      string
	ReadOffset uint64
	Off        uint32
}
