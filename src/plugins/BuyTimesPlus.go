package plugins

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"utils"
)

type ButTimesPlus struct {
	BuyRules Buyrules
}

type Order struct {

	Count    int64  `json:"count"`
	TotalAmount   float64  `json:"total_amount"`
	RealAmount	  float64  `json:"real_amount"`	
}


type Buyrules struct {
	StartDate string `json:"start"`
	EndDate   string `json:"end"`
	Count     int64  `json:"count"`
	Opration  string `json:"op"`
}

func NewBuyTimes() *ButTimesPlus {
	var BuyRules Buyrules
	this := &ButTimesPlus{BuyRules: BuyRules}
	return this
}


func (this *ButTimesPlus) Init() bool {
	return true
}


func (this *ButTimesPlus) SetRules(rules interface{}) func(value_byte interface{}) bool {
	fmt.Printf("rules : %v \n",rules)
	rule,ok:=rules.(utils.Condition)
	if !ok {
		fmt.Printf("Error rules\n")
	}
	start := strings.Split(rule.Range,",")[0]
	end := strings.Split(rule.Range,",")[1]
	total, err := strconv.ParseInt(rule.Value, 0, 0)
	if err != nil {
		fmt.Printf("Error %v \n", rule.Value)
	}
	//fmt.Printf("total : %v start : %v end : %v \n",total,start,end)
	return func(value_byte interface{}) bool{
		var err error
		buytimes := make(map[string]Order)
		body, ok := value_byte.([]byte)
		if !ok {
			fmt.Printf("Byte Error ...\n")
		}
		err = json.Unmarshal(body, &buytimes)
		if err != nil {
			fmt.Printf("Unmarshal Error ...\n")
			return false
		}
		
		var sum int64 = 0
		for date,value := range buytimes{
			//fmt.Printf("date : %v start : %v end : %v sum : %v count : %v \n",date,start,end,sum,value.Count)
			if date > start  && date < end {
				sum = sum + value.Count
			}
		}
		if sum > total {
			fmt.Printf("Match .... %v \n", buytimes)
			//fmt.Printf("Rules .... %v \n", rules)
			return true
		}
		//fmt.Printf("Not Match .... \n")
		return false
	}
}



func (this *ButTimesPlus) CustomeFunction(v1, v2 interface{}) bool {
	/*
	var err error
	var buytimes Buytimes
	body, ok := v2.([]byte)
	if !ok {
		fmt.Printf("Byte Error ...\n")
	}
	err = json.Unmarshal(body, &buytimes)
	if err != nil {
		fmt.Printf("Unmarshal Error ...\n")
		return false
	}
	var sum int64 = 0
	for i, _ := range buytimes.BuyDetail {
		if buytimes.BuyDetail[i].DateTime > "2015-03-05" {
			sum = sum + buytimes.BuyDetail[i].Count
		}
	}
	if sum > 5 {
		//fmt.Printf("Match .... %v \n", buytimes)
		return true
	}
	//fmt.Printf("Not Match .... \n")
	*/
	return false
}



//插件分词函数,返回string数组,bool参数表示是建立索引的时候还是查询的调用,STYPE = 9 调用
func (this *ButTimesPlus) SegmentFunc(value interface{},isSearch bool) []string{
	res := make([]string,0)
	if isSearch == true{
		res=append(res,fmt.Sprintf("%v",value))
		return res
	}
	
	
	fmt.Printf("SegmentFunc...\n")
	buytimes := make(map[string]Order)
	body, ok := value.(string)
	if !ok {
		fmt.Printf("Byte Error ...\n")
	}
	err := json.Unmarshal([]byte(body), &buytimes)
	if err != nil {
		fmt.Printf("Unmarshal Error ...\n")
		return nil
	}
	
	for date,value := range buytimes{
		fmt.Printf("date : %v  value  : %v \n",date,value)
		res=append(res,date)
		
	} 
	fmt.Printf("res : %v \n",res)
	return res
}


//数字分词函数,返回string数组,bool参数表示是建立索引的时候还是查询的调用,STYPE = 9 调用
func (this *ButTimesPlus) SplitNum(value interface{}) int64{
	
	return 0
}




//插件正排处理函数，建立索引的时候调用，stype =9 调用 ,返回byte数组
func (this *ButTimesPlus) BuildByteProfile(value []byte) []byte {
	
	return value
}

//插件正排处理函数，建立索引的时候调用，stype =9 调用 ,返回string,定长！！！！
func (this *ButTimesPlus) BuildStringProfile(value interface{}) string{
	
	return "nil"
} 



//插件正排处理函数，建立索引的时候调用，stype =9 调用 ,返回int64
func (this *ButTimesPlus) BuildIntProfile(value interface{}) int64{
	
	return 0
}