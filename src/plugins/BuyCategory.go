package plugins

import (
	//"encoding/json"
	//"fmt"
	//"strconv"
	//"strings"
	//"utils"
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
	
	//fmt.Printf("total : %v start : %v end : %v \n",total,start,end)
	return func(value_byte interface{}) bool{
		
		return true
	}
}


//插件分词函数,返回string数组,bool参数表示是建立索引的时候还是查询的调用,STYPE = 9 调用
func (this *BuyCategory) SegmentFunc(value interface{},isSearch bool) []string{
	return nil
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
