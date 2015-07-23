/*****************************************************************************
 *  file name : Update.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数据更新
 *
******************************************************************************/

package main

import (
	"BaseFunctions"
	"indexer"
	//"fmt"


)


type Updater struct{
	*BaseFunctions.BaseProcessor
	Indexer		*indexer.IndexSet
}


func NewUpdater(processor *BaseFunctions.BaseProcessor,indexer *indexer.IndexSet) *Updater{
	this:=&Updater{processor,indexer}
	return this
}

func (this *Updater)Process(log_id string,body []byte,params map[string]string , result map[string]interface{},ftime func(string)string) error {
	
	this.Logger.Info("Update...")
	return nil
}
