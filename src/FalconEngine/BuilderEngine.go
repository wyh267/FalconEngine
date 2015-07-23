/*****************************************************************************
 *  file name : BuilderEngine.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 索引构建
 *
******************************************************************************/

package main


import (
	"builder"
	"BaseFunctions"
	"github.com/outmana/log4jzl"
)



type BuilderEngine struct {
	DBBuilder	*builder.DBBuilder
}



func NewBuilderEngine(configure *BaseFunctions.Configure, dbadaptor *BaseFunctions.DBAdaptor, logger *log4jzl.Log4jzl, redis *BaseFunctions.RedisClient) *BuilderEngine{
	
	BaseBuilder := builder.NewBuilder(configure, dbadaptor, logger, redis)
	MyBuilder := builder.NewDBBuilder(BaseBuilder)
	this := &BuilderEngine{MyBuilder}
	return this
	
}


func (this *BuilderEngine)BuidingAllIndex(){
	
	//全量更新
	this.DBBuilder.StartBuildIndex()
	
	//启动增量更新
	
}

func (this *BuilderEngine) StartIncUpdate(){
	
}