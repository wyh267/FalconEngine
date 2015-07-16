package main

import (
	"fmt"
	"io"
	//"utils"
	//"time"
	"encoding/json"
	"bufio"
	"os"
	"indexer"
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
	
	
	
	
	ivt_idx:=utils.NewInvertIdx(utils.TYPE_TEXT,"测试索引") 
	ivt_dic:=utils.NewStringIdxDic(1000)
	
	for _,v := range Documents{
		fmt.Printf("ID : [%v]  [ %v ] \n",v.Id,v.Content)
		utils.BuildTextIndex(v.Id,v.Content,utils.RULE_EN,ivt_idx,ivt_dic)
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
	aa,_ := ti.FindTerm("aa")
	fmt.Printf("aa : %v \n",aa)
	
	bb,_ :=ti.FindTerm("and")
	fmt.Printf("and : %v \n",bb)
	
	cc,_ :=ti.FindTerm("anD")
	fmt.Printf("anD : %v \n",cc)
	
	*/
	//indexer.FindTerm("aa")
	
	
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
	

	profile:=indexer.NewTextProfile("字符串正排")
	

	for _,v := range StrDoc {
		profile.PutProfile(v.DocID,v.Value)
	}
	
	fmt.Printf("NUM_DOC : %v \n",StrDoc)
	profile.Display()
	
	
	
	
	
}



//func BuildTextIndex(doc_id int64,content string,rule int64,ivt_idx InvertIdx,ivt_dic StringIdxDic) error {