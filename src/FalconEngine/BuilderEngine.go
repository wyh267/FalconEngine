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
	"indexer"
	"github.com/outmana/log4jzl"
)



type BuilderEngine struct {
	DBBuilder	*builder.DBBuilder
}



func NewBuilderEngine(configure *BaseFunctions.Configure, dbadaptor *BaseFunctions.DBAdaptor, logger *log4jzl.Log4jzl, redis *BaseFunctions.RedisClient,index_set *indexer.IndexSet) *BuilderEngine{
	
	BaseBuilder := builder.NewBuilder(configure, dbadaptor, logger, redis,index_set)
	MyBuilder := builder.NewDBBuilder(BaseBuilder)
	this := &BuilderEngine{MyBuilder}
	return this
	
}


func (this *BuilderEngine)BuidingAllIndex(){
	
	//全量更新
	this.DBBuilder.StartBuildIndex()
	
	//启动增量更新
	//go this.DBBuilder.ScanInc()
}

func (this *BuilderEngine) StartIncUpdate(Data_chan chan builder.UpdateInfo){
	
	this.DBBuilder.ParseConfigure()
	
	go this.DBBuilder.ScanInc(Data_chan)
	
}