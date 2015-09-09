/*****************************************************************************
 *  file name : Builder.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 索引生成器
 *
******************************************************************************/

package builder

import (
	"BaseFunctions"
	"indexer"
	"utils"
)

type Builder struct {
	Configure *BaseFunctions.Configure
	Dbadaptor *BaseFunctions.DBAdaptor
	Logger    *utils.Log4FE
	RedisCli  *BaseFunctions.RedisClient
	Index_set *indexer.IndexSet
}

func NewBuilder(configure *BaseFunctions.Configure, dbadaptor *BaseFunctions.DBAdaptor, logger *utils.Log4FE, redis *BaseFunctions.RedisClient, index_set *indexer.IndexSet) *Builder {
	this := &Builder{configure, dbadaptor, logger, redis, index_set}
	return this
}
