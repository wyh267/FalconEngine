package main

import (
	"fmt"
	"io"
	"utils"
	//"time"
	//"encoding/json"
	"bufio"
	"os"
)



type Document struct {
	Content		string	`json:"content"`
	Id			int64	`json:"id"`
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
	
	
	
	
	ivt_idx.Display()
	ivt_dic.Display()
	
}



//func BuildTextIndex(doc_id int64,content string,rule int64,ivt_idx InvertIdx,ivt_dic StringIdxDic) error {