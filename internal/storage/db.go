package storage

import (
	"fmt"
	"github.com/vandathron/bcaster/internal/model"
)

type Db struct {
	topics map[string]*Partition
}

func NewDb() *Db {
	return &Db{}
}

func (db *Db) Read(topic string, offset uint32) (*model.Msg, error) {
	if _, ok := db.topics[topic]; !ok {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	return nil, nil
}

func (db *Db) Write(offset uint32, msg model.Msg) {

}
