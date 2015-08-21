/*****************************************************************************
 *  file name : TextProfile.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 文本正牌索引
 *
******************************************************************************/

package indexer

import (
	"errors"
	"fmt"
	u "utils"
)

type TextProfile struct {
	*Profile
	ProfileList []string
}

func NewTextProfile(name string) *TextProfile {
	profile := &Profile{name, 1, 1}
	this := &TextProfile{profile, make([]string, 1)}
	return this
}

func (this *TextProfile) Display() {

	fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ]============\n", this.Name, this.Len)
	for index, v := range this.ProfileList {
		fmt.Printf(" [ DOC_ID : %v ] [ VALUE : %v ] \n", index, v)
	}
	fmt.Printf(" ================================================= \n")
}

func (this *TextProfile) PutProfile(doc_id int64, value string) error {
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

func (this *TextProfile) FindValue(doc_id int64) (string, error) {
	if doc_id >= this.Len || doc_id < 1 {
		return "", errors.New("docid is wrong")
	}

	return this.ProfileList[doc_id], nil

}

func (this *TextProfile) FilterValue(doc_ids []u.DocIdInfo, value string, is_forward bool,filt_type int64) ([]u.DocIdInfo, error) {

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

	return res, nil
}

func (this *TextProfile) Put(doc_id int64, value interface{}) error {
	value_str, ok := value.(string)
	if !ok {
		return errors.New("Wrong type..")
	}

	return this.PutProfile(doc_id, value_str)

}

func (this *TextProfile) Find(doc_id int64) (interface{}, error) {

	return this.FindValue(doc_id)
}

func (this *TextProfile) Filter(doc_ids []u.DocIdInfo, value interface{}, is_forward bool,filt_type int64) ([]u.DocIdInfo, error) {

	if doc_ids == nil {
		return nil, nil
	}

	value_str, ok := value.(string)
	if !ok {
		return doc_ids, nil
	}

	return this.FilterValue(doc_ids, value_str, is_forward,filt_type)

}

func (this *TextProfile) CustomFilter(doc_ids []u.DocIdInfo, value interface{}, r bool, cf func(v1, v2 interface{}) bool) ([]u.DocIdInfo, error) {

	res := make([]u.DocIdInfo, 0, 1000)
	for i, _ := range doc_ids {
		if cf(value, this.ProfileList[doc_ids[i].DocId]) == r {
			res = append(res, doc_ids[i])
		}
	}

	return res, nil

}



func (this *TextProfile) GetType() int64 {
	return this.Type
}

