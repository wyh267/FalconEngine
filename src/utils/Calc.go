/*****************************************************************************
 *  file name : Calc.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description :
 *
******************************************************************************/

package utils

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)


type CommonStruct struct {
	Childs []Condition `json:"childs"`
	Score  int64       `json:"rule"`
}

type ConditionData struct {
	Data []CommonStruct `json:"data"`
}

type SearchInfo struct {
	Customer_id      int64         `json:"customer_id"`
	Contact_id       int64         `json:"contact_id"`
	Id               int64         `json:"_id"`
	Creator_id       int64         `json:"creator_id"`
	Last_editor_id   int64         `json:"last_editor_id"`
	Create_time      string        `json:"create_time"`
	Last_modify_time string        `json:"last_modify_time"`
	Editor_id        int64         `json:"editor_id"`
	Group_type       int64         `json:"group_type"`
	Name             string        `json:"name"`
	Conditions       ConditionData `json:"conditions"`
}

func main() {
	/*
			condition := `{
		         "name" : "智能6",
		         "editor_id" : 155,
		         "customer_id": 327,
		         "contact_id" : 0,
		         "group_type":2,
		    "conditions":{"data":[
		    {"childs":[
		        {"desc":"age","key":"user_attrib","operate":"more","value":"11"},
		       	{"desc":"sex","key":"user_attrib","operate":"equal","value":"1"},
				{"desc":"name","key":"user_attrib","operate":"equal","value":"吴"}
		        ]
		    }]}}`

			cb:=[]byte(condition)


			contact_info := map[string]string{ "age":"14","sex":"1","name":"吴英昊"  }



			fmt.Println(GroupBy(cb,contact_info))
		// {"key":"create_time","operate":"more","value":"2015-03-06"},
		//        {"key":"area","operate":"equal","value":"113"}
	*/
}

func checkTermInString(op, value string, contact_info map[string]string, field string) bool {

	return searchSubstringInField(value, contact_info[field])

}

func checkDateTime(op, value string, contact_info map[string]string, field string) bool {

	vl := strings.Split(contact_info[field], " ")
	v := vl[0]

	switch op {
	case "equal":
		return value == v
	case "unequal":
		return value != v
	case "less":
		return v < value
	case "more":
		return v > value
	default:
		return value == v
	}

	return false

}

func checkNumber(op, value string, contact_info map[string]string, field string) bool {
	value_num, err := strconv.ParseInt(value, 0, 0)
	if err != nil {
		return false
	}
	field_num, err := strconv.ParseInt(fmt.Sprintf("%v", contact_info[field]), 0, 0)
	if err != nil {
		return false
	}
	fmt.Printf("value_num : %v field_num : %v \n", value_num, field_num)
	switch op {
	case "equal":
		return value_num == field_num
	case "unequal":
		return value_num != field_num
	case "less":
		return field_num < value_num
	case "more":
		return field_num > value_num
	default:
		return value_num == field_num
	}

	return false

}

func checkAreaInfo(op, value string, contact_info map[string]string) bool {
	area_num, err := strconv.ParseInt(value, 0, 0)
	if err != nil {
		return false
	}
	area_field_num, err := strconv.ParseInt(fmt.Sprintf("%v", contact_info["from_source"]), 0, 0)
	if err != nil {
		return false
	}
	var match_num int64
	if area_num < 1000 {
		match_num = area_field_num / 100
	} else {
		match_num = area_field_num
	}

	if (match_num == area_num && op == "equal") || (match_num != area_num && op != "equal") {
		return true
	} else {
		return false
	}

}

func checkSourceInfo(op, value string, contact_info map[string]string) bool {
	var value_num int64
	source_value, err := strconv.ParseInt(fmt.Sprintf("%v", contact_info["source"]), 0, 0)
	if err != nil {
		return false
	}
	switch value {
	case "addbyadmin":
		value_num = 1
	case "export":
		value_num = 2
	case "unknown":
		value_num = 3
	default:
		value_num = 3
	}

	if (op == "equal" && value_num == source_value) || (op != "equal" && value_num != source_value) {
		return true
	} else {
		return false
	}

}

func checkSMSInfo(op, value string, contact_info map[string]string) bool {
	var Query string
	var Field string
	switch op {
	case "click":
		Query = value + "_" + "1"
		Field = "sms_click"
	case "send":
		Query = value + "_" + "1"
		Field = "sms_sended"
	case "unclick":
		Query = value + "_" + "0"
		Field = "sms_click"
	case "unsend":
		Query = value + "_" + "0"
		Field = "sms_sended"
	default:

	}
	return searchSubstringInField(Query, contact_info[Field])
}

func checkMailInfo(op, value string, contact_info map[string]string) bool {

	var Query string
	var Field string
	switch op {
	case "look": //查看
		Query = value + "_" + "1"
		Field = "email_view"
	case "click":
		Query = value + "_" + "1"
		Field = "email_click"
	case "send":
		Query = value + "_" + "1"
		Field = "email_sended"
	case "unlook":
		Query = value + "_" + "0"
		Field = "email_view"
	case "unclick":
		Query = value + "_" + "0"
		Field = "email_click"
	case "unsend":
		Query = value + "_" + "0"
		Field = "email_sended"
	}

	return searchSubstringInField(Query, contact_info[Field])
}

func checkAllConditions(vv Condition, ContactInfo map[string]string) bool {
	var result bool
	switch vv.Key {
	case "user_attrib":
		switch vv.Desc {
		case "age":
			result = checkNumber(vv.Op, vv.Value, ContactInfo, "age")
		case "sex":
			result = checkNumber(vv.Op, vv.Value, ContactInfo, "sex")
		case "name":
			result = checkTermInString(vv.Op, vv.Value, ContactInfo, "name")
		case "email":
			result = checkTermInString(vv.Op, vv.Value, ContactInfo, "email")
		case "mobile_phone":
			result = checkTermInString(vv.Op, vv.Value, ContactInfo, "mobile_phone")
		case "is_customer":
			result = checkNumber(vv.Op, vv.Value, ContactInfo, "is_customer")
		case "birth":
			result = checkDateTime(vv.Op, vv.Value, ContactInfo, "birth")
		case "address":
			result = checkTermInString(vv.Op, vv.Value, ContactInfo, "address")
		case "zip":
			result = checkTermInString(vv.Op, vv.Value, ContactInfo, "zip")
		case "job_title":
			result = checkTermInString(vv.Op, vv.Value, ContactInfo, "job_title")
		case "company":
			result = checkTermInString(vv.Op, vv.Value, ContactInfo, "company")
		case "website":
			result = checkTermInString(vv.Op, vv.Value, ContactInfo, "website")
		case "annual_revenue":
			result = checkNumber(vv.Op, vv.Value, ContactInfo, "annual_revenue")
		case "industry":
			result = checkTermInString(vv.Op, vv.Value, ContactInfo, "industry")
		}
	case "mail":
		result = checkMailInfo(vv.Op, vv.Value, ContactInfo)
	case "sms":
		result = checkSMSInfo(vv.Op, vv.Value, ContactInfo)
	case "area":
		result = checkAreaInfo(vv.Op, vv.Value, ContactInfo)
	case "source":
		result = checkSourceInfo(vv.Op, vv.Value, ContactInfo)
	case "email_client":
	case "create_time":
		result = checkDateTime(vv.Op, vv.Value, ContactInfo, "create_time")
	case "update_time":
		result = checkDateTime(vv.Op, vv.Value, ContactInfo, "update_time")
	case "score":
		result = checkNumber(vv.Op, vv.Value, ContactInfo, "score")
	case "buy_count":
		result = checkBuyTimes(vv, ContactInfo, "buy_times")
	case "buy_amount":
		result = checkBuyTimes(vv, ContactInfo, "buy_times")
	case "has_buy":
		switch vv.Desc {
			case "buy_products":
				result = checkBuyProductsOrCategorys(vv, ContactInfo, "buy_products")
			case "buy_category":	
				result = checkBuyProductsOrCategorys(vv, ContactInfo, "buy_categorys")
		}
	case "last_buy_date":
		result = checkDateTime(vv.Op, vv.Value, ContactInfo, "last_buy_date")

	}

	return result

}


func stringInList(target string,sources []string) bool {
	
	for _,source := range sources{
		if target == source {
			return true
		}
	}
	return false
}


func checkBuyProductsOrCategorys(rule Condition, contact_info map[string]string, field string) bool {
	
	var start,end string
	date_range := strings.Split(rule.Range,",")
	if len(date_range) != 2{
		start= "2015-01-01"
		end = "2015-12-31"
	}
	start = date_range[0]
	end = date_range[1]
	products := RemoveDuplicatesAndEmpty(strings.Split(rule.Value, ";"))
	//fmt.Printf("total : %v start : %v end : %v \n",total,start,end)

	productsInfo := make(map[string][]string)
	
	err := json.Unmarshal([]byte(contact_info[field]), &productsInfo)
	if err != nil {
		fmt.Printf("Unmarshal Error ...\n")
		return false
	}
	list :=make([]string,0)
	for k,v := range productsInfo{
		
		if k>end || k < start {
			continue
		}
		list = append(list,v...)
	}
	
	for _,p := range products{
		if stringInList(p,list) == false{
			return false 
		}
	}
	
	return true
	
	
}


type utilOrder struct {

	Count    int64  `json:"count"`
	TotalAmount   float64  `json:"total_amount"`
	RealAmount	  float64  `json:"real_amount"`	
}

func checkBuyTimes(rule Condition, contact_info map[string]string, field string) bool {
	var start,end string
	date_range := strings.Split(rule.Range,",")
	if len(date_range) != 2{
		start= "2015-01-01"
		end = "2015-12-31"
	}
	start = date_range[0]
	end = date_range[1]
	var total_count int64
	var total_amount float64
	var err error
	if rule.Key == "buy_count"{
		total_count, err = strconv.ParseInt(rule.Value, 0, 0)
		if err != nil {
			fmt.Printf("Error %v \n", rule.Value)
			return false
		}
	}else{
		total_amount, err = strconv.ParseFloat(rule.Value,0)
		if err != nil {
			fmt.Printf("Error %v \n", rule.Value)
			return false
		}		
	}
	
	buytimes := make(map[string]utilOrder)
	err = json.Unmarshal([]byte(contact_info[field]), &buytimes)
	if err != nil {
		fmt.Printf("Unmarshal Error ...\n")
		return false
	}
	
	var sum int64 = 0
	var sum_amount float64 = 0.0
	for date,value := range buytimes{
		if date > start  && date < end {
			sum = sum + value.Count
			sum_amount = sum_amount + value.RealAmount
		}
	}
	//fmt.Printf("start : %v end : %v sum : %v  \n",start,end,sum)
	switch rule.Op{
		case "more":
	//		fmt.Printf("more, %v \n",((sum > total_count && rule.Key == "buy_count") || (sum_amount > total_amount && rule.Key == "buy_amount")))
			return ((sum > total_count && rule.Key == "buy_count") || (sum_amount > total_amount && rule.Key == "buy_amount"))
		case "less":
			return ((sum < total_count && rule.Key == "buy_count") || (sum_amount < total_amount && rule.Key == "buy_amount"))
		case "equal":
			return ((sum == total_count && rule.Key == "buy_count") || (sum_amount == total_amount && rule.Key == "buy_amount"))
	}

	return false
	
	
	
	
}


func searchSubstringInField(query, fieldstr string) bool {
	return strings.Contains(fieldstr, query)
}

/*
func GroupBy(body []byte,ContactInfo map[string]string) bool {
	var searchInfo SearchInfo
	err := json.Unmarshal(body,&searchInfo)
	if err != nil {
		return false
	}

	//第一层循环
	for _,data := range searchInfo.Conditions.Data{
		sub_flag := false
		//第二层循环，只要满足一个循环即可
		for _,vv := range data.Childs{
			if checkAllConditions(vv,ContactInfo) == true {
				sub_flag = true
				continue
			}else{
				sub_flag = false
				break
			}

		}

		if sub_flag == true {
			return true
		}
	}

	return false

}
*/

func ComputScore(body []byte, ContactInfo map[string]string) (int64, error) {
	var searchInfo SearchInfo
	err := json.Unmarshal(body, &searchInfo)
	if err != nil {
		return 0, err
	}

	//contact_score, err := strconv.ParseInt(fmt.Sprintf("%v", ContactInfo["score"]), 0, 0)
	//if err != nil {
	//	contact_score = 0
	//}
	//contact_score := 0

	//第一层循环
	for _, data := range searchInfo.Conditions.Data {
		sub_flag := false
		//第二层循环，只要满足一个循环即可
		for _, vv := range data.Childs {
			if checkAllConditions(vv, ContactInfo) == true {
				sub_flag = true
				continue
			} else {
				sub_flag = false
				break
			}

		}

		if sub_flag == true {
			//fmt.Printf("HAHAHAHAHAHA: %v \n",data.Score)
			return data.Score, nil
		}
	}

	return 0, nil

}
