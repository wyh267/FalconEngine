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
	"fmt"
	"errors"
)

type NumberProfile struct{
	Name  		string
	Len			int64
	ProfileList	[]int64
}


func NewNumberProfile(name string) *NumberProfile{
	this := &NumberProfile{name,1,make([]int64,1)}
	return this
}


func (this *NumberProfile)Display(){
	
	fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ]============\n",this.Name,this.Len)
	for index,v := range this.ProfileList{
		fmt.Printf(" [ DOC_ID : %v ] [ VALUE : %v ] \n",index,v)
	}
	fmt.Printf(" ================================================= \n")
}


func (this *NumberProfile)PutProfile(doc_id,value int64) error {
	if doc_id > this.Len || doc_id < 1{
		return errors.New("docid is wrong")
	}
	
	if doc_id == this.Len {
		this.ProfileList = append(this.ProfileList,value)
		this.Len++
		return nil
	}
	
	this.ProfileList[doc_id] = value
	return nil
	
}


func (this *NumberProfile)FindValue(doc_id int64) (int64,error) {
	if doc_id >= this.Len || doc_id < 1{
		return 0,errors.New("docid is wrong")
	}
	
	return this.ProfileList[doc_id],nil
	
}

