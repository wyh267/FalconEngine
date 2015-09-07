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

type Condition struct {
	Key   string `json:"key"`
	Op    string `json:"operate"`
	Value string `json:"value"`
	Desc  string `json:"desc"`
}

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
		return v  > value
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

	}

	return result

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
			return data.Score, nil
		}
	}

	return 0, nil

}
