package BaseFunctions

import (
	"utils"

)

type BaseProcessor struct {
	Configure      *Configure
	Logger         *utils.Log4FE
	DbAdaptor      *DBAdaptor
	RedisCli       *RedisClient
	RemoteRedisCli *RedisClient
}

type FEProcessor interface {
	Process(log_id string, body []byte, params map[string]string, result map[string]interface{}, ftime func(string) string) error
}
