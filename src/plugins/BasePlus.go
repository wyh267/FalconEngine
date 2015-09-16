package plugins

import (
	"fmt"
	u "utils"
)






func NewPlus(plus_name string) u.CustomInterface {

	switch plus_name {
	case "buy_times":
		fmt.Printf("NewBuyTimes\n ")
		return NewBuyTimes()
	case "buy_products":
		fmt.Printf("NewBuyProducts....\n")
		return NewBuyProducts()
	case "buy_category":
		fmt.Printf("NewBuyCategory...\n")
		return NewBuyCategory()
	default:
		return NewBase()
	}

}

type BasePlus struct {
	Cid int64 `json:"cid"`
}

func NewBase() *BasePlus {

	this := &BasePlus{999}
	return this
}


//作废
func (this *BasePlus) CustomeFunction(v1, v2 interface{}) bool {
	return true
}

//插件初始化函数，只调用一次
func (this *BasePlus) Init() bool {
	return true
}


//插件正排自定义函数，只调用一次，设置rule，返回一个函数，以后每次判断的时候调用返回的回调函数
func (this *BasePlus) SetRules(rules interface{}) func(value_byte interface{}) bool {
	
	
	
	return func(value_byte interface{}) bool{
		fmt.Printf("Rules : %v \n",rules)
		fmt.Printf("value_byte : %v \n",value_byte)
		return true
		
	}
}



//插件分词函数,返回string数组,bool参数表示是建立索引的时候还是查询的调用,STYPE = 9 调用
func (this *BasePlus) SegmentFunc(value interface{},isSearch bool) []string{
	
	return nil
}


//数字分词函数,返回string数组,bool参数表示是建立索引的时候还是查询的调用,STYPE = 9 调用
func (this *BasePlus) SplitNum(value interface{}) int64{
	
	return 0
}




//插件正排处理函数，建立索引的时候调用，stype =9 调用 ,返回byte数组
func (this *BasePlus) BuildByteProfile(value []byte) []byte {
	
	return value
}

//插件正排处理函数，建立索引的时候调用，stype =9 调用 ,返回string,定长！！！！
func (this *BasePlus) BuildStringProfile(value interface{}) string{
	
	return "nil"
} 



//插件正排处理函数，建立索引的时候调用，stype =9 调用 ,返回int64
func (this *BasePlus) BuildIntProfile(value interface{}) int64{
	
	return 0
}


