

package builder

import (
	"fmt"
	"strings"
	"errors"
	"indexer"
	"utils"
	"strconv"
)

type FieldInfo struct {
	IsPK	bool
	IsIvt	bool
	IsPlf	bool
	FType	string
	Name	string
	IvtIdx		*utils.InvertIdx
	IvtStrDic	*utils.StringIdxDic
	IvtNumDic	*utils.NumberIdxDic
	PlfText		*indexer.TextProfile
	PlfNumber	*indexer.NumberProfile			
}



type DBBuilder struct{
	*Builder
	sql			string
	Fields		[]FieldInfo
}


func NewDBBuilder(b *Builder) *DBBuilder{
	this := &DBBuilder{b,"",make([]FieldInfo,0)}
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
		fi.Name = k
		
		this.Fields=append(this.Fields,fi)
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
	
	var fields string
	for index,v := range this.Fields{
		//构造sql语句
		fields = fields + "," + v.Name	
		//构造索引数据指针
		if v.IsIvt {
			
			if v.FType == "T"{
				this.Fields[index].IvtIdx=utils.NewInvertIdx(utils.TYPE_TEXT,v.Name) 
				this.Fields[index].IvtStrDic =  utils.NewStringIdxDic(10000)
				
				
			}
			
			if v.FType == "N"{
				this.Fields[index].IvtIdx=utils.NewInvertIdx(utils.TYPE_NUM,v.Name) 
				this.Fields[index].IvtNumDic = utils.NewNumberIdxDic(10000)
				
			}
		}
		
		
		if v.IsPlf {
			if v.FType == "T"{
				this.Fields[index].PlfText = indexer.NewTextProfile(v.Name)
			}
			
			if v.FType == "N"{
				this.Fields[index].PlfNumber = indexer.NewNumberProfile(v.Name)
			}
			
			
		}
		
		
	}
	
	sql := fmt.Sprintf(this.sql,fields[1:len(fields)])
	fmt.Printf("SQL :: %v \n", sql)
	
	rows, err := this.Dbadaptor.QueryFormat(sql)
	if err != nil {
		this.Logger.Error(" %v",err)
		return err
	}
	defer rows.Close()
	var doc_id int64
	doc_id=1
	segment:= utils.NewSegmenter("./data/dictionary.txt")
	builder := &utils.IndexBuilder{segment}
	
	
	for rows.Next() {
		//values := make([]interface{},len(this.Fields))
		values := make([]interface{}, len(this.Fields))
    	writeCols := make([]string, len(this.Fields))
    	for i, _ := range writeCols {
        	values[i] = &writeCols[i]
   		 }
		
		err := rows.Scan(values...)
		if err != nil {
			this.Logger.Error("SQL ERROR : %v",err)
			return err
		}
		
		for index,v := range writeCols{
			//v,_ := value.(string)
			if this.Fields[index].IsIvt {
				
				if this.Fields[index].FType == "T" {
					err:=builder.BuildTextIndex(doc_id,v,this.Fields[index].IvtIdx,this.Fields[index].IvtStrDic)
					if err !=nil{
						this.Logger.Error("ERROR : %v",err)
					}
				}
				
				if this.Fields[index].FType == "N" {
					v_num, err := strconv.ParseInt(v, 0, 0)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}

					err=builder.BuildNumberIndex(doc_id,v_num,this.Fields[index].IvtIdx,this.Fields[index].IvtNumDic)
					if err !=nil{
						this.Logger.Error("ERROR : %v",err)
					}
				}
				
				
			}
			
			if this.Fields[index].IsPlf {
				
				if this.Fields[index].FType == "T" {
				
					err := this.Fields[index].PlfText.PutProfile(doc_id,v)
					if err !=nil{
						this.Logger.Error("ERROR : %v",err)
					}
				}
				
				if this.Fields[index].FType == "N" {
					v_num, err := strconv.ParseInt(v, 0, 0)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
					err = this.Fields[index].PlfNumber.PutProfile(doc_id,v_num)
					if err !=nil{
						this.Logger.Error("ERROR : %v",err)
					}
				}
				
				
			}
			
			
			
		}
		doc_id ++
		this.Logger.Info("DOC_ID : %v  VALUE : %v",doc_id,writeCols)
		
	}
	
	
	for index,fields := range this.Fields {
		
		if this.Fields[index].IsIvt {
			
				utils.WriteToJson(fields.IvtIdx,fmt.Sprintf("./index/%v_idx.json",fields.Name))	
				if this.Fields[index].FType == "T" {
					
					utils.WriteToJson(fields.IvtStrDic,fmt.Sprintf("./index/%v_dic.json",fields.Name))
					
					
				}
				
				if this.Fields[index].FType == "N" {
					
					utils.WriteToJson(fields.IvtNumDic,fmt.Sprintf("./index/%v_dic.json",fields.Name))
				}
				
		}
		
		if this.Fields[index].IsPlf {
				
				if this.Fields[index].FType == "T" {
					
					utils.WriteToJson(fields.PlfText,fmt.Sprintf("./index/%v_pfl.json",fields.Name))
					
				}
		
				if this.Fields[index].FType == "N" {
					
					utils.WriteToJson(fields.PlfNumber,fmt.Sprintf("./index/%v_pfl.json",fields.Name))
					
				}
				
		}
		
	}
	
	
	
	
	/*
	
	var i int64
	i=1
	
	id_idx:=utils.NewInvertIdx(utils.TYPE_NUM,"id") 
	id_dic:=utils.NewNumberIdxDic(10000)
	id_pfl:=indexer.NewNumberProfile("id")
	
	
	cid_idx:=utils.NewInvertIdx(utils.TYPE_NUM,"cid") 
	cid_dic:=utils.NewNumberIdxDic(10000)
	cid_pfl:=indexer.NewNumberProfile("cid")
	
	name_idx:=utils.NewInvertIdx(utils.TYPE_TEXT,"name") 
	name_dic:=utils.NewStringIdxDic(10000)
	
	email_idx:=utils.NewInvertIdx(utils.TYPE_TEXT,"email") 
	email_dic:=utils.NewStringIdxDic(10000)
	
	addr_idx:=utils.NewInvertIdx(utils.TYPE_TEXT,"address") 
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
	
	utils.WriteToJson(id_idx,"./index/id_idx.json")
	utils.WriteToJson(id_dic,"./index/id_dic.json")
	utils.WriteToJson(id_pfl,"./index/id_pfl.json")
	utils.WriteToJson(cid_idx,"./index/cid_idx.json")
	utils.WriteToJson(cid_dic,"./index/cid_dic.json")
	utils.WriteToJson(cid_pfl,"./index/cid_pfl.json")
	utils.WriteToJson(name_idx,"./index/name_idx.json")
	utils.WriteToJson(name_dic,"./index/name_dic.json")
	utils.WriteToJson(email_idx,"./index/email_idx.json")
	utils.WriteToJson(email_dic,"./index/email_dic.json")
	utils.WriteToJson(addr_idx,"./index/address_idx.json")
	utils.WriteToJson(addr_dic,"./index/address_dic.json")

	*/
	return nil
	
	
	
}

	

