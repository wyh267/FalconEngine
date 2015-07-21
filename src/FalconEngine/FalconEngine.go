package main

import (
	//"fmt"
	//"io"
	//"utils"
	//"time"
	//"encoding/json"
	//"bufio"
	//"os"
	//"errors"
	"indexer"
	//"strings"
	"BaseFunctions"
	"flag"
	"fmt"
	"github.com/outmana/log4jzl"
	"builder"
	//"github.com/huichen/sego"
)



type Document struct {
	Content		string	`json:"content"`
	Id			int64	`json:"id"`
}



type NumDocument struct {
	DocID		int64 `json:"docid"`
	Value		int64 `json:"value"`
}




func main(){
	
	fmt.Printf("init FalconEngine.....\n")
	//读取启动参数
	var configFile string
	var search	   string
	var err error
	flag.StringVar(&configFile, "conf", "search.conf", "configure file full path")
	flag.StringVar(&search, "mode", "search", "start mode[search | build ]")
	flag.Parse()

	//读取配置文件
	configure, err := BaseFunctions.NewConfigure(configFile)
	if err != nil {
		fmt.Printf("[ERROR] Parse Configure File Error: %v\n", err)
		return
	}

	//启动日志系统
	logger, err := log4jzl.New("ProxyServer")
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
	
	
	//初始化ID生成器
	redisClient, err := BaseFunctions.NewRedisClient(configure, logger)
	if err != nil {
		fmt.Printf("[ERROR] Create redisClient Error: %v\n", err)
		return
	}
	defer redisClient.Release()
	
	
	if search == "search"{
		fields,err := configure.GetTableFields()
		if err != nil{
			logger.Error("%v",err)
			return 
		}
		index_set := indexer.NewIndexSet(logger)
		index_set.InitIndexSet(fields)
	
		fmt.Println("INDEX SET : " ,index_set)
		//index_set.Display()
		
		//res,_ := index_set.SearchField("吴英昊","name")
		ruls := make(map[string]interface{})
		ruls["query"] = "吴英昊15810589078"
		ruls["cid"] = int64(146)
		//ruls["mobile_phone"] = "18511078600"
		
		fruls := make([]indexer.FilterRule,0)
		//fruls = append(fruls,indexer.FilterRule{"cid",true,int64(146)})
		fruls = append(fruls,indexer.FilterRule{"last_modify_time",true,"2015-07-10 15:36:47"})
		
		
		
		//ruls["cid"] = int64(146)
		res,_ := index_set.SearchByRule(ruls)
		//res,_ := index_set.CustomFilter
		
		//res,_ = index_set.FilterByRules(res,fruls)
		//res,_ := index_set.Search("wuyinghao")
		cf := func(v1,v2 interface{})(bool){
			v11,_:=v1.(string)
			v22,_:=v2.(string)
			if v11 == v22 {
				return true
			}
			return false
		}
		
		res,_ = index_set.FilterByCustom(res,"last_modify_time","2015-07-10 15:36:47",true,cf)
		fmt.Printf("RES : %v ",res)
		
	}else if search == "build" {
		BaseBuilder := builder.NewBuilder(configure,dbAdaptor,logger,redisClient)
		MyBuilder := builder.NewDBBuilder(BaseBuilder)
		MyBuilder.StartBuildIndex()
	}else{
		logger.Error("Wrong start mode...only support [ search | build ]")
	}

	
	

	/*

	BaseBuilder := builder.NewBuilder(configure,dbAdaptor,logger)
	MyBuilder := builder.NewDBBuilder(BaseBuilder)
	MyBuilder.StartBuildIndex()
	
	a,err := configure.GetTableFields()
	if err != nil {
		fmt.Println(err)
	}
	
	fmt.Println("a:",a)

	*/
/*	
	s:=utils.NewStaticHashTable(10)
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.PutKeyForInt("abc"))
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.PutKeyForInt("abc"))
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.PutKeyForInt("abc"))
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.PutKeyForInt("abc"))
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.PutKeyForInt("ddfe"))
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.PutKeyForInt("ac"))
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.PutKeyForInt("ad"))
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.PutKeyForInt("adfdsss"))
	
	
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.FindKey("ac"))
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.FindKey("ddfe"))
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.FindKey("abc"))
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),s.FindKey("zzz"))
	
	
	utils.WriteToJson(s,"./a.json")
	
	sdata,_:=utils.ReadFromJson("./a.json")
	
	var info utils.StaticHashTable
	err := json.Unmarshal(sdata, &info)
	if err != nil {
		fmt.Printf("ERR")
	}
	
	
	fmt.Printf("%v [INFO]  %v\n",time.Now().Format("2006-01-02 15:04:05"),info)
*/
	
	/*
	Documents := make([]Document,0)
	f,_:=os.Open("./test.dat")
	defer f.Close()
	buff := bufio.NewReader(f)
	var id int64
	id=1
	for {
		var doc Document
		line,err := buff.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		doc.Content=line[:len(line)-1]
		doc.Id=id
		Documents=append(Documents,doc)
		id++
		
	}
	
	segment:= utils.NewSegmenter("./data/dictionary.txt")
	builder := &utils.IndexBuilder{segment}
	
	ivt_idx:=utils.NewInvertIdx(utils.TYPE_TEXT,"测试索引") 
	ivt_dic:=utils.NewStringIdxDic(1000)
	
	for _,v := range Documents{
		fmt.Printf("ID : [%v]  [ %v ] \n",v.Id,v.Content)
		builder.BuildTextIndex(v.Id,v.Content,ivt_idx,ivt_dic)
	}
	
	
	utils.WriteToJson(ivt_idx,"./ivt_idx.json")
	utils.WriteToJson(ivt_dic,"./ivt_dic.json")
	
	bidx,_:=utils.ReadFromJson("./ivt_idx.json")
	bdic,_:=utils.ReadFromJson("./ivt_dic.json")
	
	
	var idx utils.InvertIdx
	err := json.Unmarshal(bidx, &idx)
	if err != nil {
		fmt.Printf("ERR")
	}
	
	
	var dic utils.StringIdxDic
	err = json.Unmarshal(bdic, &dic)
	if err != nil {
		fmt.Printf("ERR")
	}
	
	
	idx.Display()
	dic.Display()
	
	
	ti :=indexer.NewTextIndex("text_indexTest",&idx,&dic)
	
	
	aa,_ := ti.FindTerm("我们")
	fmt.Printf("我们 : %v \n",aa)
	
	bb,_ :=ti.FindTerm("and")
	fmt.Printf("and : %v \n",bb)
	
	cc,_ :=ti.FindTerm("anD")
	fmt.Printf("anD : %v \n",cc)
	
	
	//indexer.FindTerm("aa")
	*/
/*	
	
	NumDoc := make([]NumDocument,0)
	f,_:=os.Open("./testnum.dat")
	defer f.Close()
	buff := bufio.NewReader(f)
	for {
		
		line,err := buff.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		
		
		var doc NumDocument
		err = json.Unmarshal([]byte(line), &doc)
		if err != nil {
			fmt.Printf("ERR")
		}
		
		
		NumDoc=append(NumDoc,doc)
		
	}
	
	ivt_idx:=utils.NewInvertIdx(utils.TYPE_NUM,"数字测试索引") 
	ivt_dic:=utils.NewNumberIdxDic(1000)
	profile:=indexer.NewNumberProfile("数字正排")
	

	for _,v := range NumDoc {
		utils.BuildNumberIndex(v.DocID,v.Value,ivt_idx,ivt_dic)
		profile.PutProfile(v.DocID,v.Value)
	}
	
	fmt.Printf("NUM_DOC : %v \n",NumDoc)
	ivt_idx.Display()
	ivt_dic.Display()
	profile.Display()
	
	ti :=indexer.NewNumberIndex("munber_indexTest",ivt_idx,ivt_dic)
	aa,_ := ti.FindNumber(77)
	fmt.Printf("77 : %v \n",aa)
	
	bb,_ :=ti.FindNumber(24)
	fmt.Printf("24 : %v \n",bb)
	
	cc,_ :=ti.FindNumber(46334)
	fmt.Printf("46334 : %v \n",cc)
	
*/

/*
	type StrDocument struct {
	DocID		int64 `json:"docid"`
	Value		string `json:"value"`
	}
	
	StrDoc := make([]StrDocument,0)
	f,_:=os.Open("./teststr.dat")
	defer f.Close()
	buff := bufio.NewReader(f)
	for {
		
		line,err := buff.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		
		
		var doc StrDocument
		err = json.Unmarshal([]byte(line), &doc)
		if err != nil {
			fmt.Printf("ERR")
		}
		
		
		StrDoc=append(StrDoc,doc)
		
	}
	

	profile:=indexer.NewTextProfile("字符串正排",1)
	

	for _,v := range StrDoc {
		profile.PutProfile(v.DocID,v.Value)
	}
	
	fmt.Printf("NUM_DOC : %v \n",StrDoc)
	
	utils.WriteToJson(profile,"./profile.json")
	
	bprofile,_:=utils.ReadFromJson("./profile.json")
	
	var pr indexer.TextProfile
	err := json.Unmarshal(bprofile, &pr)
	if err != nil {
		fmt.Printf("ERR")
	}
	
	pr.Display()
	
	docids := []utils.DocIdInfo{{1,0},{2,0},{3,0},{5,0},{7,0},{10,0},{12,0}}
	fmt.Printf("%v\n",docids)
	docids,_=pr.FilterValue(docids,"24",true)
	fmt.Printf("%v\n",docids)
	*/
	
	
	
	/*
	var segmenter sego.Segmenter
    segmenter.LoadDictionary("./data/dictionary.txt")

    // 分词
    text := []byte("this is  a the website 12341123 吴英昊")
    segments := segmenter.Segment(text)

    // 处理分词结果
    // 支持普通模式和搜索模式两种分词，见代码中SegmentsToString函数的注释。
    fmt.Println(sego.SegmentsToSlice(segments, true)) 
	
	*/
	
	
}



//func BuildTextIndex(doc_id int64,content string,rule int64,ivt_idx InvertIdx,ivt_dic StringIdxDic) error {