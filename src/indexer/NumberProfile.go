/*****************************************************************************
 *  file name : NumberProfile.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数字正排文件
 *
******************************************************************************/

package indexer

import (
	"errors"
	"fmt"
	"strconv"
	u "utils"
)

type NumberProfile struct {
	*Profile
	ProfileList []int64
}

func NewNumberProfile(name string) *NumberProfile {
	profile := &Profile{name, 2, 1}
	this := &NumberProfile{profile, make([]int64, 1)}
	return this
}

func (this *NumberProfile) Display() {

	fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ]============\n", this.Name, this.Len)
	for index, v := range this.ProfileList {
		fmt.Printf(" [ DOC_ID : %v ] [ VALUE : %v ] \n", index, v)
	}
	fmt.Printf(" ================================================= \n")
}

func (this *NumberProfile) PutProfile(doc_id, value int64) error {
	//fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ] [ DOC_ID : %v ]============\n", this.Name, this.Len,doc_id)
	if doc_id > this.Len || doc_id < 1 {
		return errors.New("docid is wrong")
	}

	if doc_id == this.Len {
		this.ProfileList = append(this.ProfileList, value)
		this.Len++
		return nil
	}

	this.ProfileList[doc_id] = value
	return nil

}

func (this *NumberProfile) FindValue(doc_id int64) (int64, error) {
	if doc_id >= this.Len || doc_id < 1 {
		return 0, errors.New("docid is wrong")
	}

	return this.ProfileList[doc_id], nil

}

func (this *NumberProfile) FilterValue(doc_ids []u.DocIdInfo, value int64, is_forward bool, filt_type int64) ([]u.DocIdInfo, error) {

	res := make([]u.DocIdInfo, 0, 1000)

	switch filt_type {
	case FILT_TYPE_LESS:

		for i, _ := range doc_ids {

			if this.ProfileList[doc_ids[i].DocId] < value {
				res = append(res, doc_ids[i])
			}
		}
	case FILT_TYPE_ABOVE:
		for i, _ := range doc_ids {
			if this.ProfileList[doc_ids[i].DocId] > value {
				res = append(res, doc_ids[i])
			}
		}
	case FILT_TYPE_EQUAL:
		for i, _ := range doc_ids {
			if this.ProfileList[doc_ids[i].DocId] == value {
				res = append(res, doc_ids[i])
			}
		}
	case FILT_TYPE_UNEQUAL:
		for i, _ := range doc_ids {
			if this.ProfileList[doc_ids[i].DocId] != value {
				res = append(res, doc_ids[i])
			}
		}
	default:
		for i, _ := range doc_ids {
			if this.ProfileList[doc_ids[i].DocId] == value {
				res = append(res, doc_ids[i])
			}
		}
	}

	/*
		if is_forward == true {

			for i, _ := range doc_ids {
				if this.ProfileList[doc_ids[i].DocId] == value {
					res = append(res, doc_ids[i])
				}
			}

		} else {
			for i, _ := range doc_ids {
				if this.ProfileList[doc_ids[i].DocId] != value {
					res = append(res, doc_ids[i])
				}
			}
		}
	*/
	return res, nil
}

func (this *NumberProfile) Put(doc_id int64, value interface{}) error {
	value_num, ok := value.(int64)
	if !ok {
		return errors.New("Wrong type..")
	}

	return this.PutProfile(doc_id, value_num)

}

func (this *NumberProfile) Find(doc_id int64) (interface{}, error) {

	return this.FindValue(doc_id)
}

func (this *NumberProfile) Filter(doc_ids []u.DocIdInfo, value interface{}, is_forward bool, filt_type int64) ([]u.DocIdInfo, error) {

	if doc_ids == nil {
		return nil, nil
	}

	value_str, ok := value.(string)
	if ok {
		v, err := strconv.ParseInt(value_str, 0, 0)
		if err != nil {
			fmt.Printf("Error %v \n", value)
			return doc_ids, nil
		}
		return this.FilterValue(doc_ids, v, is_forward, filt_type)
	}

	value_num, ok := value.(int64)
	if ok {
		return this.FilterValue(doc_ids, value_num, is_forward, filt_type)
	}

	value_num_float, ok := value.(float64)
	if ok {
		return this.FilterValue(doc_ids, int64(value_num_float), is_forward, filt_type)
	}

	return doc_ids, nil

}

func (this *NumberProfile) CustomFilter(doc_ids []u.DocIdInfo, value interface{}, r bool, cf func(v1, v2 interface{}) bool) ([]u.DocIdInfo, error) {

	res := make([]u.DocIdInfo, 0, 1000)
	for i, _ := range doc_ids {
		if cf(value, this.ProfileList[doc_ids[i].DocId]) == r {
			res = append(res, doc_ids[i])
		}
	}

	return res, nil
}

func (this *NumberProfile) GetType() int64 {
	return this.Type
}
