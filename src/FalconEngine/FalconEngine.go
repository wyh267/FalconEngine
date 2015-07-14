package main

import (
	"fmt"
	"utils"
	"time"
	"encoding/json"
)


func main(){
	
	fmt.Printf("init FalconEngine.....\n")
	
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
	
}