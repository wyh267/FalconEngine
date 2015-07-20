/*****************************************************************************
 *  file name : IndexSet.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 倒排索引的集合对象
 *
******************************************************************************/


package indexer


import (
	"fmt"
	"errors"
	"utils"
	"strings"
	"encoding/json"
	"github.com/outmana/log4jzl"
)



type IndexSet struct {
	Logger            *log4jzl.Log4jzl
	IvtIndex	map[string]IndexInterface
	PflIndex	map[string]ProfileInterface
	
}



func NewIndexSet (logger *log4jzl.Log4jzl) *IndexSet {
	
	this := &IndexSet{IvtIndex:make(map[string]IndexInterface),Logger:logger,PflIndex:make(map[string]ProfileInterface)}
	return this

}


func (this *IndexSet) PutIndex(name string,index IndexInterface) error {
	
	this.IvtIndex[name] = index 
	return nil
}

func (this *IndexSet) PutProfile(name string,profile ProfileInterface) error {
	
	this.PflIndex[name] = profile 
	return nil
}




func (this *IndexSet) GetProfile(name string) (ProfileInterface,error) {
	
	return this.PflIndex[name],nil
}

func (this *IndexSet) GetIndex(name string) (IndexInterface,error) {
	
	return this.IvtIndex[name],nil
}



func (this *IndexSet) GetAll() (map[string]IndexInterface ,error ) {
	return this.IvtIndex,nil
}


func (this *IndexSet) InitIndexSet(fields map[string]string) error {
	for k,v := range fields {	
		l:=strings.Split(v,",")
		if len(l) != 4 {
			this.Logger.Error("%v",errors.New("Wrong config file"))
			return errors.New("Wrong configure for index")
		}
			
		if l[1] == "1" {
			idx_name := fmt.Sprintf("./index/%v_idx.json",k)
			dic_name := fmt.Sprintf("./index/%v_dic.json",k)
			bidx,_:=utils.ReadFromJson(idx_name)
			bdic,_:=utils.ReadFromJson(dic_name)
			var idx utils.InvertIdx
			err := json.Unmarshal(bidx, &idx)
			if err != nil {
				return err
			}
			this.Logger.Info("Loading Index     [ %v ] ...",idx_name)
			if l[3] == "T" {//text ivt
				var dic utils.StringIdxDic
				this.Logger.Info("Loading Index Dic [ %v ] type : Text ...",dic_name)
				err = json.Unmarshal(bdic, &dic)
				if err != nil {
					return err
				}
				index := NewTextIndex(k,&idx,&dic)
				this.PutIndex(k,index)
				
			}else{//number ivt
				var dic utils.NumberIdxDic
				this.Logger.Info("Loading Index Dic [ %v ] type : Number...",dic_name)
				err = json.Unmarshal(bdic, &dic)
				if err != nil {
					return err
				}
				index := NewNumberIndex(k,&idx,&dic)
				this.PutIndex(k,index)
			}
			
		}
		
		
		
		if l[2] == "1" {
			pfl_name := fmt.Sprintf("./index/%v_pfl.json",k)
			bpfl,_:=utils.ReadFromJson(pfl_name)
			
			if l[3] == "T" {
				var pfl TextProfile
				this.Logger.Info("Loading Index Profile [ %v ] type : Text ...",pfl_name)
				err := json.Unmarshal(bpfl, &pfl)
				if err != nil {
					return err
				}
				this.PutProfile(k,&pfl)
				
			}else{
				
				var pfl NumberProfile
				this.Logger.Info("Loading Index Profile [ %v ] type : Number ...",pfl_name)
				err := json.Unmarshal(bpfl, &pfl)
				if err != nil {
					return err
				}
				this.PutProfile(k,&pfl)
				
			}
			
		}
		
		
		
	}
	return nil
}



func (this *IndexSet) Display() {
	
	for _,v := range this.IvtIndex {
		v.Display()
	}
	
	for _,v := range this.PflIndex {
		v.Display()
	}
	
}


