package main

import (
	"os"
	"indexer"
	"BaseFunctions"
	"flag"
	"fmt"
	"github.com/outmana/log4jzl"
	"net/http"
	"utils"
	"builder"
	"runtime"
)



func main() {

	fmt.Printf("init FalconEngine.....\n")
	//读取启动参数
	var configFile string
	var search string
	var cores int
	var err error
	flag.StringVar(&configFile, "conf", "search.conf", "configure file full path")
	flag.StringVar(&search, "mode", "search", "start mode[ search | build ]")
	flag.IntVar(&cores,"core",4,"cpu cores")
	flag.Parse()

	runtime.GOMAXPROCS(cores)
	//读取配置文件
	configure, err := BaseFunctions.NewConfigure(configFile)
	if err != nil {
		fmt.Printf("[ERROR] Parse Configure File Error: %v\n", err)
		return
	}

	//启动日志系统
	logger, err := log4jzl.New("FalconEngine")
	if err != nil {
		fmt.Printf("[ERROR] Create logger Error: %v\n", err)
		//return
	}

	//初始化数据库适配器
	dbAdaptor, err := BaseFunctions.NewDBAdaptor(configure, logger)
	if err != nil {
		fmt.Printf("[ERROR] Create DB Adaptor Error: %v\n", err)
		return
	}
	defer dbAdaptor.Release()

	//初始化本地redis
	redisClient, err := BaseFunctions.NewRedisClient(configure, logger)
	if err != nil {
		fmt.Printf("[ERROR] Create redisClient Error: %v\n", err)
		return
	}
	defer redisClient.Release()

	//初始化远程redis
	remoteRedisClient, err := BaseFunctions.NewRemoteRedisClient(configure, logger)
	if err != nil {
		fmt.Printf("[ERROR] Create redisClient Error: %v\n", err)
		return
	}
	defer remoteRedisClient.Release()

	if search == "search" {
		
		processor := &BaseFunctions.BaseProcessor{configure,logger,dbAdaptor,redisClient,remoteRedisClient}
		bitmap := utils.NewBitmap()
		fields, err := configure.GetTableFields()
		if err != nil {
			logger.Error("%v", err)
			return
		}
		index_set := indexer.NewIndexSet(bitmap,logger)
		index_set.InitIndexSet(fields)
		
		
		searcher :=  NewSearcher(processor,index_set) // &Searcher{processor}
		data_chan:=make(chan builder.UpdateInfo,1000)
		updater := NewUpdater(processor,index_set,data_chan)
		updater.IncUpdating()
		router := &BaseFunctions.Router{configure,logger,map[string]BaseFunctions.FEProcessor{
			"search":	searcher,
			"update":	updater,
		}}
		
		
		builder := NewBuilderEngine(configure, dbAdaptor, logger, redisClient,index_set)
		builder.StartIncUpdate(data_chan)
		
		logger.Info("Server Start...")
		port, _ := configure.GetPort()
		addr := fmt.Sprintf(":%d", port)
		err = http.ListenAndServe(addr, router)
		if err != nil {
			logger.Error("Server start fail: %v", err)
			os.Exit(1)
		}
		

	} else if search == "build" {
		
		builder := NewBuilderEngine(configure, dbAdaptor, logger, redisClient,nil)
		builder.BuidingAllIndex()
	} else {
		logger.Error("Wrong start mode...only support [ search | build ]")
	}

}


