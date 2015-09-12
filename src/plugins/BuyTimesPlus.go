package plugins

import (
	"encoding/json"
	"fmt"
)

type ButTimesPlus struct {
	BuyTimes Buytimes
	BuyRules Buyrules
}

type Order struct {
	DateTime string `json:"date"`
	Count    int64  `json:"count"`
	Amount   int64  `json:"amount"`
}

type Buytimes struct {
	Cid       int64   `json:"cid"`
	BuyDetail []Order `json:"detail"`
}

type Buyrules struct {
	StartDate string `json:"start"`
	EndDate   string `json:"end"`
	Count     int64  `json:"count"`
	Opration  string `json:"op"`
}

func NewBuyTimes() *ButTimesPlus {
	var BuyTimes Buytimes
	var BuyRules Buyrules
	this := &ButTimesPlus{BuyTimes: BuyTimes, BuyRules: BuyRules}
	return this
}


func (this *ButTimesPlus) Init() bool {
	return true
}


func (this *ButTimesPlus) SetRules(rules interface{}) func(value_byte interface{}) bool {
	
	return func(value_byte interface{}) bool{
		var err error
		var buytimes Buytimes
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
		for i, _ := range buytimes.BuyDetail {
			if buytimes.BuyDetail[i].DateTime > "2015-03-05" {
				sum = sum + buytimes.BuyDetail[i].Count
			}
		}
		if sum > 5 {
			//fmt.Printf("Match .... %v \n", buytimes)
			//fmt.Printf("Rules .... %v \n", rules)
			return true
		}
		//fmt.Printf("Not Match .... \n")
		return false
	}
}



func (this *ButTimesPlus) CustomeFunction(v1, v2 interface{}) bool {
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
	return false
}
