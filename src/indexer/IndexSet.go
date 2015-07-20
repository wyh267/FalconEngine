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
	Segmenter	*utils.Segmenter
	
}



func NewIndexSet (logger *log4jzl.Log4jzl) *IndexSet {
	segment:= utils.NewSegmenter("./data/dictionary.txt")
	this := &IndexSet{Segmenter:segment,IvtIndex:make(map[string]IndexInterface),Logger:logger,PflIndex:make(map[string]ProfileInterface)}
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



func (this *IndexSet) SearchString(query string) ([]utils.DocIdInfo,error) {
	
	
	//按照最大切分进行切词
	terms := utils.RemoveDuplicatesAndEmpty(this.Segmenter.Segment(query,false))
	
	//首先按照字段检索
	//交集结果
	var res_list []utils.DocIdInfo
	for key,_ := range this.IvtIndex {
		if this.IvtIndex[key].GetType() != 0 {
			continue
		}
			
		for index,term := range terms {
			l,ok := this.IvtIndex[key].Find(term)
			if !ok {
				break
			}
			this.Logger.Info("[Term : %v ] [Field: %v ] DocIDs : %v",term,key,l)
			//求交集
			if index==0{
				res_list = l
			}else{
				res_list,ok = utils.Interaction(l,res_list)
				if !ok{
					break
				}
			}
			
		}
		if len(res_list)>0{
			return res_list,nil
		}
		
	}
	
	return nil,nil
	
	
	//如果数量不够，跨字段检索
	
}



func (this *IndexSet) SearchNumber(query int64) ([]utils.DocIdInfo,error) {
	
	return nil,nil
}


func (this *IndexSet) Search(query interface{}) ([]utils.DocIdInfo,error) {
	
	query_str,ok := query.(string)
	if ok {
		return this.SearchString(query_str)
	}
	
	query_num,ok := query.(int64)
	if ok {
		return this.SearchNumber(query_num)
	}

	return nil,errors.New("Type Error")
}

