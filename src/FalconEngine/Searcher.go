/*****************************************************************************
 *  file name : Searcher.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 搜索引擎
 *
******************************************************************************/


package main

import (
	"BaseFunctions"
	"indexer"
)



type Searcher struct{
	*BaseFunctions.BaseProcessor
	Indexer		*indexer.IndexSet
}


func NewSearcher(processor *BaseFunctions.BaseProcessor,indexer *indexer.IndexSet) *Searcher{
	this:=&Searcher{processor,indexer}
	return this
}



func (this *Searcher)Process(log_id string,body []byte,params map[string]string , result map[string]interface{},ftime func(string)string) error {
	
	
	this.Logger.Info("[LOG_ID:%v]Running Searcher ....Time: %v ",log_id,ftime("A"))
	this.Logger.Info("[LOG_ID:%v]Running Searcher ....Time: %v ",log_id,ftime("hello"))
	this.Logger.Info("[LOG_ID:%v]Running Searcher ....Time: %v ",log_id,ftime("world"))
	this.Logger.Info("[LOG_ID:%v]Running Searcher ....Time: %v ",log_id,ftime("ttttt"))
	//this.Indexer.Display()
	
	return nil
}








