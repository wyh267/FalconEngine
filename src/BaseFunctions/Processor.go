package BaseFunctions

import (
	"github.com/outmana/log4jzl"
)

type BaseProcessor struct {
	Configure      *Configure
	Logger         *log4jzl.Log4jzl
	DbAdaptor      *DBAdaptor
	RedisCli       *RedisClient
	RemoteRedisCli *RedisClient
}

type FEProcessor interface {
	Process(log_id string, body []byte, params map[string]string, result map[string]interface{}, ftime func(string) string) error
}
