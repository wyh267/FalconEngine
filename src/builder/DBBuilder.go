

package builder

import (
	//"fmt"
	"strings"
	"errors"
	"indexer"
	"utils"
)

type FieldInfo struct {
	IsPK	bool
	IsIvt	bool
	IsPlf	bool
	FType	string
}



type DBBuilder struct{
	*Builder
	sql			string
	Fields		map[string]FieldInfo
}


func NewDBBuilder(b *Builder) *DBBuilder{
	this := &DBBuilder{b,"",make(map[string]FieldInfo)}
	return this
}




func (this *DBBuilder) parseConfigure() error {
	this.Logger.Info("Parse Configure File ..... ")
	var err error
	this.sql,err = this.Configure.GetSqlSentence()
	if err != nil{
		this.Logger.Error("%v",err)
		return err
	}
	
	fields,err := this.Configure.GetTableFields()
	if err != nil{
		this.Logger.Error("%v",err)
		return err
	}
	
	for k,v := range fields {
		
		l:=strings.Split(v,",")
		if len(l) != 4 {
			this.Logger.Error("%v",errors.New("Wrong config file"))
			return errors.New("Wrong config file")
		}
		var fi FieldInfo
		if l[0] == "1"{
			fi.IsPK=true
		}else{
			fi.IsPK=false
		}
		
		if l[1] == "1"{
			fi.IsIvt=true
		}else{
			fi.IsIvt=false
		}
		
		if l[2] == "1"{
			fi.IsPlf=true
		}else{
			fi.IsPlf=false
		}
		
		fi.FType=l[3]
		
		this.Fields[k]=fi
	}
	
	this.Logger.Info("SELF : %v",*this)
	return nil
	
}



func (this *DBBuilder) StartBuildIndex(){
	
	this.Logger.Info("Start Building Index ..... ")
	
	this.parseConfigure()
	
	this.Buiding()
	
	//fmt.Println("Start Building Index",yy)
}

type DBDocument struct {
	DocId		int64
	Id			int64
	Cid			int64
	Name		string
	Email		string	
	Address		string
}

func (this *DBBuilder)Buiding () error {
	
	rows, err := this.Dbadaptor.QueryFormat(this.sql)
	if err != nil {
		this.Logger.Error(" %v",err)
		return err
	}
	defer rows.Close()
	var i int64
	i=1
	
	id_idx:=utils.NewInvertIdx(utils.TYPE_NUM,"id") 
	id_dic:=utils.NewNumberIdxDic(10000)
	id_pfl:=indexer.NewNumberProfile("id",2)
	
	
	cid_idx:=utils.NewInvertIdx(utils.TYPE_NUM,"cid") 
	cid_dic:=utils.NewNumberIdxDic(10000)
	cid_pfl:=indexer.NewNumberProfile("cid",2)
	
	name_idx:=utils.NewInvertIdx(utils.TYPE_TEXT,"name") 
	name_dic:=utils.NewStringIdxDic(10000)
	
	email_idx:=utils.NewInvertIdx(utils.TYPE_TEXT,"email") 
	email_dic:=utils.NewStringIdxDic(10000)
	
	addr_idx:=utils.NewInvertIdx(utils.TYPE_TEXT,"addr") 
	addr_dic:=utils.NewStringIdxDic(10000)
	
	segment:= utils.NewSegmenter("./data/dictionary.txt")
	builder := &utils.IndexBuilder{segment}
	
	for rows.Next() {
		var v DBDocument
		err := rows.Scan(&v.Id,&v.Cid,&v.Name,&v.Email,&v.Address)
		if err != nil {
			return err
		}
		v.DocId = i
		i++
		builder.BuildTextIndex(v.DocId,v.Name,name_idx,name_dic)
		builder.BuildTextIndex(v.DocId,v.Email,email_idx,email_dic)
		builder.BuildTextIndex(v.DocId,v.Address,addr_idx,addr_dic)
		builder.BuildNumberIndex(v.DocId,v.Id,id_idx,id_dic)
		builder.BuildNumberIndex(v.DocId,v.Cid,cid_idx,cid_dic)
		id_pfl.PutProfile(v.DocId,v.Id)
		cid_pfl.PutProfile(v.DocId,v.Cid)
	}
	
	utils.WriteToJson(id_idx,"./id_idx.json")
	utils.WriteToJson(id_dic,"./id_dic.json")
	utils.WriteToJson(id_pfl,"./id_pfl.json")
	utils.WriteToJson(cid_idx,"./cid_idx.json")
	utils.WriteToJson(cid_dic,"./cid_dic.json")
	utils.WriteToJson(cid_pfl,"./cid_pfl.json")
	utils.WriteToJson(name_idx,"./name_idx.json")
	utils.WriteToJson(name_dic,"./name_dic.json")
	utils.WriteToJson(email_idx,"./email_idx.json")
	utils.WriteToJson(email_dic,"./email_dic.json")
	utils.WriteToJson(addr_idx,"./addr_idx.json")
	utils.WriteToJson(addr_dic,"./addr_dic.json")

	
	return nil
	
	
	
}

	

