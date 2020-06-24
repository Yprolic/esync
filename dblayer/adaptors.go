package dblayer

import (
	"github.com/sirupsen/logrus"
)

// adaptor 提供直接操作数据库的封装
type adaptor interface {
	Connect(c *Connection) error
	Close() error

	SetOrUpdate(request *SetOrUpdateRequest) error
	Delete(request *DeleteRequest) error
	Backup(request *BackupRequest) error
	Load(request *LoadRequest) error
	ExistSchema(request *ExistSchemaRequest) (bool, error)
}

type Record struct {
	ID   string      `json:"id"`
	Data interface{} `json:"data"`
}

type Connection struct {
	User     string
	Password string
	Address  string
}

type Location struct {
	T          DBType
	Connection Connection
}

type SetOrUpdateRequest struct {
	Data  []*Record
	Index string
	Type  string
}

type DeleteRequest struct {
	Index string
	Type  string
	Key   string
	Value interface{}
}

type BackupRequest struct {
	Dir   string
	Index string
	Type  string
}

type LoadRequest struct {
	Dir   string
	Index string
	Type  string
}

type ExistSchemaRequest struct {
	Schema string
}

type DBType string

const (
	Elasticsearch DBType = "Elasticsearch"
)

var (
	adaptors = make(map[DBType]adaptor)
)

func init() {
	Register(Elasticsearch, &ElasticSearch{})
}

func Register(at DBType, ad adaptor) {
	if _, exist := adaptors[at]; exist {
		logrus.Panicf("adaptor %s already exists", at)
	}
	adaptors[at] = ad
}

func Get(at DBType) (adaptor, bool) {
	v, exist := adaptors[at]
	return v, exist
}
