package plus


import (
	"fmt"
	"indexer"
)


type ButTimesPlus struct{
	
	BuyTimes 	Buytimes
	BuyRules	Buyruls
}


type Detail struct{
	DateTime	string `json:"date"`
	Count		int64  `json:"count"`
}

type Buytimes struct{
	Cid		int64	`json:"cid"`
	BuyDetail	[]Detail	`json:"detail"`
	
}



type Buyrules struct{
	
	StartDate	string `json:"start"`
	EndDate		string `json:"end"`
	Count		int64  `json:"count"`
	Opration	string `json:"op"`
	
}




func (this *ButTimesPlus)CustomeFunction(v1, v2 interface{}) bool {
	
	return false
}
