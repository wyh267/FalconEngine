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
	//"errors"
	"builder"
)

type Updater struct {
	*BaseFunctions.BaseProcessor
	Indexer   *indexer.IndexSet
	Data_chan chan builder.UpdateInfo
}

func NewUpdater(processor *BaseFunctions.BaseProcessor, indexer *indexer.IndexSet, data_chan chan builder.UpdateInfo) *Updater {

	this := &Updater{processor, indexer, data_chan}
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

func (this *Updater) Process(log_id string, body []byte, params map[string]string, result map[string]interface{}, ftime func(string) string) error {

	this.Logger.Info("Update...")
	var updateInfo builder.UpdateInfo
	info := make(map[string]string)
	info["id"] = "154"
	info["cid"] = "146"
	info["name"] = "吴坚"
	info["email"] = "hello@aa.com"
	info["address"] = "ABCADDRESS"
	info["city"] = "Changsha"
	info["country"] = "USA"
	info["sex"] = "1"
	info["mobile_phone"] = "13232"
	info["last_modify_time"] = "2015-01-01 00:11:22"
	updateInfo.Info = info
	updateInfo.UpdateType = 1
	updateInfo.ErrChan = make(chan error)
	this.Data_chan <- updateInfo

	errinfo := <-updateInfo.ErrChan

	if errinfo != nil {
		this.Logger.Info("Update Fail.... %v ", errinfo)
	} else {
		this.Logger.Info("Update success....")
	}
	//this.Indexer.UpdateRecord(info,false)
	//this.Indexer.Display()
	return nil
}

func (this *Updater) IncUpdating() {

	go this.updatingThread()

}

func (this *Updater) updatingThread() {
	this.Logger.Info("Start Inc Updating Recive Now ..... ")
	for {
		select {
		case info := <-this.Data_chan:
			//this.Logger.Info("Got data ... %v ",info)
			info.ErrChan <- this.Indexer.UpdateRecord(info.Info, info.UpdateType)

		}

	}
}
