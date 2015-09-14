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

func (this *BasePlus) CustomeFunction(v1, v2 interface{}) bool {
	return true
}


func (this *BasePlus) Init() bool {
	return true
}


func (this *BasePlus) SetRules(rules interface{}) func(value_byte interface{}) bool {
	
	
	
	return func(value_byte interface{}) bool{
		fmt.Printf("Rules : %v \n",rules)
		fmt.Printf("value_byte : %v \n",value_byte)
		return true
		
	}
}