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
	"fmt"
	"errors"
)


type TextProfile struct{
	*Profile
	ProfileList	[]string
}


func NewTextProfile(name string) *TextProfile{
	profile := &Profile{name,1}
	this := &TextProfile{profile,make([]string,1)}
	return this
}

func (this *TextProfile)Display(){
	
	fmt.Printf(" ========== [ NAME : %v ] [ LEN : %v ]============\n",this.Name,this.Len)
	for index,v := range this.ProfileList{
		fmt.Printf(" [ DOC_ID : %v ] [ VALUE : %v ] \n",index,v)
	}
	fmt.Printf(" ================================================= \n")
}

func (this *TextProfile)PutProfile(doc_id int64,value string) error {
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


func (this *TextProfile)FindValue(doc_id int64) (string,error) {
	if doc_id >= this.Len || doc_id < 1{
		return "",errors.New("docid is wrong")
	}
	
	return this.ProfileList[doc_id],nil
	
}

