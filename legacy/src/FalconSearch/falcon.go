package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"runtime"
	//"runtime/debug"
	//"tree"
	fd "FalconDispatcher"
	fe "FalconEngine"
	fs "FalconService"
	"utils"
)

func main() {

	var cores int
	var mport int
	var lport int
	var master int
	var localip string
	var masterip string
	flag.IntVar(&cores, "core", runtime.NumCPU(), "CPU 核心数量")
	flag.IntVar(&lport, "p", 9991, "启动端口，默认9991")
	flag.IntVar(&master, "m", 0, "启动master，默认启动的为searcher")
	flag.StringVar(&localip, "lip", "127.0.0.1", "本机ip地址，默认127.0.0.1")
	flag.StringVar(&masterip, "mip", "127.0.0.1", "主节点ip地址，默认127.0.0.1")
	flag.IntVar(&mport, "mp", 9990, "主节点端口，默认9990")
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	//debug.SetGCPercent(0)
	fmt.Printf("CORES:%v\n", runtime.NumCPU())
	//启动日志系统
	logger, err := utils.New("FalconSearcher")
	if err != nil {
		fmt.Printf("[ERROR] Create logger Error: %v\n", err)
		return
	}

	//初始化Manager
	var engine utils.Engine
	if master == 0 {
		//初始化分词器
		logger.Info("[INFO] Loading Segmenter ...")
		utils.GSegmenter = utils.NewSegmenter("./data/dictionary.txt")
		//utils.GOptions = utils.NewPy(utils.STYLE_NORMAL, utils.NO_SEGMENT)
		logger.Info("[INFO] Init Search Engine ...")
		engine = fe.NewDefaultEngine(localip, masterip, lport, mport, logger)
	} else {
		logger.Info("[INFO] Init Dispatcher ...")
		engine = fd.NewDispatcher(localip, logger)
	}

	//启动性能监控
	//go func() {
	//	log.Println(http.ListenAndServe(":6060", nil))
	//}()

	utils.GetDocIDsChan, utils.GiveDocIDsChan = utils.DocIdsMaker()

	//启动全局缓存
	// utils.Cache,err = cache.NewCache("memory", `{"interval":60}`)
	// if err != nil {
	//     fmt.Printf("[ERROR] Create Cache Error: %v\n", err)
	//		return
	// }

	logger.Info("[INFO] Starting FalconEngine Service.....")
	http := fs.NewHttpService(engine, lport, logger)

	http.Start()

}
