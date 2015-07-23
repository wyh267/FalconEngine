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
/*

cid = 		0,1,1,N
name =  	0,1,0,T
email =  	0,1,0,T
address = 	0,1,0,T
city = 		0,1,1,T
country = 	0,1,1,T
sex		=	0,1,1,N
mobile_phone= 0,1,1,T
last_modify_time = 0,1,1,T
*/
func (this *Updater)Process(log_id string,body []byte,params map[string]string , result map[string]interface{},ftime func(string)string) error {
	
	this.Logger.Info("Update...")
	info := make(map[string]string)
	info["id"]="154"
	info["cid"]="146"
	info["name"]="吴坚"
	info["email"]="hello@aa.com"
	info["address"]="ABCADDRESS"
	info["city"]="Changsha"
	info["country"]="USA"
	info["sex"]="1"
	info["mobile_phone"]="13232"
	info["last_modify_time"]="2015-01-01 00:11:22"	
	this.Indexer.UpdateRecord(info,false)
	//this.Indexer.Display()
	return nil
}
