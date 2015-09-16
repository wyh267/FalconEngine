package plugins

import (
	"encoding/json"
	"fmt"
	//"strconv"
	"strings"
	"utils"
)

type BuyCategory struct {

}


func NewBuyCategory() *BuyCategory {
	this := &BuyCategory{}
	return this
}


func (this *BuyCategory) Init() bool {
	return true
}


func (this *BuyCategory) SetRules(rules interface{}) func(value_byte interface{}) bool {
	
	rule,ok:=rules.(utils.Condition)
	if !ok {
		fmt.Printf("Error rules\n")
	}
	var start,end string
	date_range := strings.Split(rule.Range,",")
	if len(date_range) != 2{
		start= "2015-01-01"
		end = "2015-12-31"
	}
	start = date_range[0]
	end = date_range[1]
	categorys := utils.RemoveDuplicatesAndEmpty(strings.Split(rule.Value, ";"))



	return func(value_byte interface{}) bool{
		
		categorysInfo := make(map[string][]string)
		body, ok := value_byte.([]byte)
		if !ok {
			fmt.Printf("Byte Error ...\n")
			return false 
		}
		err := json.Unmarshal(body, &categorysInfo)
		if err != nil {
			fmt.Printf("Unmarshal Error ...\n")
			return false
		}
		list :=make([]string,0)
		for k,v := range categorysInfo{
			
			if k>end || k < start {
				continue
			}
			list = append(list,v...)
		}
		
		for _,p := range categorys {
			if StringInList(p,list) == false{
				return false 
			}
		}
		
		return true
		
	}
}


//插件分词函数,返回string数组,bool参数表示是建立索引的时候还是查询的调用,STYPE = 9 调用
func (this *BuyCategory) SegmentFunc(value interface{},isSearch bool) []string{
	
	res := make([]string,0)
	if isSearch == true{
		return utils.RemoveDuplicatesAndEmpty(strings.Split(fmt.Sprintf("%v",value), ";"))
	}
	
	
	categorysInfo := make(map[string][]string)
	body, ok := value.(string)
	if !ok {
		fmt.Printf("Byte Error ...\n")
		return nil
	}
	err := json.Unmarshal([]byte(body), &categorysInfo)
	if err != nil {
		fmt.Printf("Unmarshal Error ...\n")
		return nil
	}
	
	for _,value := range categorysInfo{
		//fmt.Printf("date : %v  value  : %v \n",date,value)
		res=append(res,value...)
		
	} 
	//fmt.Printf("BuyProducts SegmentFunc res : %v \n",res)
	
	return res
}




//插件正排处理函数，建立索引的时候调用，stype =9 调用 ,返回byte数组
func (this *BuyCategory) BuildByteProfile(value []byte) []byte {
	
	return value
}








//插件正排处理函数，建立索引的时候调用，stype =9 调用 ,返回string,定长！！！！
func (this *BuyCategory) BuildStringProfile(value interface{}) string{
	
	return "nil"
} 



//插件正排处理函数，建立索引的时候调用，stype =9 调用 ,返回int64
func (this *BuyCategory) BuildIntProfile(value interface{}) int64{
	
	return 0
}



//数字分词函数,返回string数组,bool参数表示是建立索引的时候还是查询的调用,STYPE = 9 调用
func (this *BuyCategory) SplitNum(value interface{}) int64{
	
	return 0
}




//作废
func (this *BuyCategory) CustomeFunction(v1, v2 interface{}) bool {
	
	return false
}
