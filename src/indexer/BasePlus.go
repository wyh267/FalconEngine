
package indexer


import (
	"fmt"
)



func NewPlus(plus_name string) CustomInterface {
	
	switch plus_name{
		case "buy_times":
			fmt.Printf("NewBuyTimes\n ")
			return NewBuyTimes()
		default :
			return NewBase()
	}
	
	
}


type BasePlus struct {
	Cid		int64 `json:"cid"`
}


func NewBase() *BasePlus {
	
	this := &BasePlus{999}
	return this
}


func (this *BasePlus)CustomeFunction(v1, v2 interface{}) bool {
	return true
}