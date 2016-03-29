package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	//"runtime/debug"
	//"tree"
	fe "FalconEngine"
	fs "FalconService"
	"utils"
)

func main() {

	var cores int
	var port int
	var config string
	var idxfile string
	var datafile string
	var new int
	var closetime int
	flag.IntVar(&cores, "core", runtime.NumCPU(), "CPU 核心数量")
	flag.IntVar(&port, "p", 9990, "启动端口")
	flag.StringVar(&config, "c", "config.json", "配置文件位置")
	flag.StringVar(&datafile, "d", "data.log", "数据文件")
	flag.StringVar(&idxfile, "i", "idx.log", "索引文件")
	flag.IntVar(&new, "r", 0, "是否从日志恢复数据")
	flag.IntVar(&closetime, "t", 600, "数据库关闭时间")
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

	//初始化分词器
	logger.Info("[INFO] Loading Segmenter ...")
	utils.GSegmenter = utils.NewSegmenter("./data/dictionary.txt")

	//初始化Manager
	engine := fe.NewDefaultEngine(logger)

	//启动性能监控
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
    
    
    utils.GetDocIDsChan, utils.GiveDocIDsChan = utils.DocIdsMaker()

	//启动全局缓存
	// utils.Cache,err = cache.NewCache("memory", `{"interval":60}`)
	// if err != nil {
	//     fmt.Printf("[ERROR] Create Cache Error: %v\n", err)
	//		return
	// }

	logger.Info("[INFO] Starting FalconEngine Service.....")
	http := fs.NewHttpService(engine, logger)
	http.Start()

}
