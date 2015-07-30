package builder

import (
	"errors"
	"fmt"
	"indexer"
	"strconv"
	"strings"
	"utils"
	"time"

	//"os"
)

type FieldInfo struct {
	IsPK      bool
	IsIvt     bool
	IsPlf     bool
	FType     string
	Name      string
	IvtIdx    *utils.InvertIdx
	IvtStrDic *utils.StringIdxDic
	IvtNumDic *utils.NumberIdxDic
	PlfText   *indexer.TextProfile
	PlfNumber *indexer.NumberProfile
}

type DBBuilder struct {
	*Builder
	sql    string
	Fields []FieldInfo
}

func NewDBBuilder(b *Builder) *DBBuilder {
	this := &DBBuilder{b, "", make([]FieldInfo, 0)}
	return this
}


/*****************************************************************************
*  function name : parseConfigure
*  params : 
*  return : 
*
*  description : 解析配置文件，填充FieldInfo结构体数组
*
******************************************************************************/
func (this *DBBuilder) ParseConfigure() error {
	this.Logger.Info("Parse Configure File ..... ")
	var err error
	this.sql, err = this.Configure.GetSqlSentence()
	if err != nil {
		this.Logger.Error("%v", err)
		return err
	}

	fields, err := this.Configure.GetTableFields()
	if err != nil {
		this.Logger.Error("%v", err)
		return err
	}

	for k, v := range fields {

		l := strings.Split(v, ",")
		if len(l) != 4 {
			this.Logger.Error("%v", errors.New("Wrong config file"))
			return errors.New("Wrong config file")
		}
		var fi FieldInfo
		if l[0] == "1" {
			fi.IsPK = true
		} else {
			fi.IsPK = false
		}

		if l[1] == "1" {
			fi.IsIvt = true
		} else {
			fi.IsIvt = false
		}

		if l[2] == "1" {
			fi.IsPlf = true
		} else {
			fi.IsPlf = false
		}

		fi.FType = l[3]
		fi.Name = k

		this.Fields = append(this.Fields, fi)
	}

	//this.Logger.Info("SELF : %v",*this)
	return nil

}

func (this *DBBuilder) StartBuildIndex() {

	this.Logger.Info("Start Building Index ..... ")

	this.ParseConfigure()

	this.Buiding()
	//for {
	
		
	//}
	//fmt.Println("Start Building Index",yy)
}




type UpdateInfo struct {
	Info		map[string]string
	IsProfile	bool
	ErrChan		chan error
}

func (this *DBBuilder) ScanInc(Data_chan chan UpdateInfo) error {
	
	this.Logger.Info("Start Inc Updating Now ..... ")
	
	curr_time:= time.Now().Format("2006-01-02 15:04:05")
	var fields string
	for _, v := range this.Fields {
		//构造sql语句
		fields = fields + "," + v.Name
	}
	
	incSql,_ := this.Configure.GetIncSql()
	incField,_:= this.Configure.GetIncField()
	for {
	sql := fmt.Sprintf(incSql, fields[1:len(fields)],curr_time)
	//fmt.Printf("SQL :: %v \n", sql)
	rows, err := this.Dbadaptor.QueryFormat(sql)
	if err != nil {
		this.Logger.Error(" %v", err)
		return err
	}
	defer rows.Close()
	for rows.Next() {
		isUpdate := false
		//values := make([]interface{},len(this.Fields))
		values := make([]interface{}, len(this.Fields))
		writeCols := make([]string, len(this.Fields))
		for i, _ := range writeCols {
			values[i] = &writeCols[i]
		}

		err := rows.Scan(values...)
		if err != nil {
			this.Logger.Error("SQL ERROR : %v", err)
			return err
		}
		new_values := make(map[string]string)
		for index, v := range writeCols {
			new_values[this.Fields[index].Name] = v
			
		}
		//this.Logger.Info("New : %v ",new_values)
		fieldlist:=make([]string,0)
		for k,_ := range new_values{
			fieldlist=append(fieldlist,k)
		}
		
		redis_map,err:=this.RedisCli.GetFields(new_values["id"],fieldlist)
		if err != nil {
			if err.Error() == "redigo: nil returned"{
				this.Logger.Info("Old continue : %v  ERR : %v ",redis_map,err.Error())
				isUpdate=true
			}else{
				continue
			}
		}
		//this.Logger.Info("Old : %v  ERR : %v ",redis_map,err)
		for k,v := range redis_map{
			vv,ok := new_values[k]
			if !ok{
				break
			}
			//this.Logger.Info("K : %v ==== V : %v === VV : %v",k,v,vv)
			if (v != vv) && k != incField {
				isUpdate = true
				curr_time = new_values[incField]
				break
			}
		}
		
		if isUpdate{
			this.Logger.Info("Must Update ,Old : %v ",redis_map)
			this.Logger.Info("Must Update ,New : %v ",new_values)
			this.RedisCli.SetFields(0, new_values)
			upinfo := UpdateInfo{new_values,false,make(chan error)}
			Data_chan <- upinfo
			errinfo:= <-upinfo.ErrChan
			if errinfo != nil {
				this.Logger.Info("Update Fail.... %v ", errinfo)
			}else{
				this.Logger.Info("Update success....")
			}
		}
		
	}
	time.Sleep(5000 * time.Millisecond)
	}
	
	
	
	return nil
}




/*****************************************************************************
*  function name : Buiding
*  params : 
*  return : 
*
*  description : 构建索引文件，并将detail存入redis中，这个函数太啰嗦了。
*
******************************************************************************/
func (this *DBBuilder) Buiding() error {

	var fields string
	for index, v := range this.Fields {
		//构造sql语句
		fields = fields + "," + v.Name
		//构造索引数据指针
		if v.IsIvt {

			if v.FType == "T" {
				this.Fields[index].IvtIdx = utils.NewInvertIdx(utils.TYPE_TEXT, v.Name)
				this.Fields[index].IvtStrDic = utils.NewStringIdxDic(20021)

			}

			if v.FType == "N" {
				this.Fields[index].IvtIdx = utils.NewInvertIdx(utils.TYPE_NUM, v.Name)
				this.Fields[index].IvtNumDic = utils.NewNumberIdxDic(20021)

			}
		}

		if v.IsPlf {
			if v.FType == "T" {
				this.Fields[index].PlfText = indexer.NewTextProfile(v.Name)
			}

			if v.FType == "N" {
				this.Fields[index].PlfNumber = indexer.NewNumberProfile(v.Name)
			}

		}

	}

	sql := fmt.Sprintf(this.sql, fields[1:len(fields)])
	fmt.Printf("SQL :: %v \n", sql)

	rows, err := this.Dbadaptor.QueryFormat(sql)
	if err != nil {
		this.Logger.Error(" %v", err)
		return err
	}
	defer rows.Close()
	var doc_id int64
	doc_id = 1
	segment := utils.NewSegmenter("./data/dictionary.txt")
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
			this.Logger.Error("SQL ERROR : %v", err)
			return err
		}
		redis_map := make(map[string]string)
		for index, v := range writeCols {
			redis_map[this.Fields[index].Name] = v
			//v,_ := value.(string)
			if this.Fields[index].IsIvt {

				if this.Fields[index].FType == "T" {
					err := builder.BuildTextIndex(doc_id, v, this.Fields[index].IvtIdx, this.Fields[index].IvtStrDic)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
				}

				if this.Fields[index].FType == "N" {
					v_num, err := strconv.ParseInt(v, 0, 0)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}

					err = builder.BuildNumberIndex(doc_id, v_num, this.Fields[index].IvtIdx, this.Fields[index].IvtNumDic)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
				}

			}

			if this.Fields[index].IsPlf {

				if this.Fields[index].FType == "T" {

					err := this.Fields[index].PlfText.PutProfile(doc_id, v)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
				}

				if this.Fields[index].FType == "N" {
					v_num, err := strconv.ParseInt(v, 0, 0)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
					err = this.Fields[index].PlfNumber.PutProfile(doc_id, v_num)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
				}

			}

		}

		this.RedisCli.SetFields(doc_id, redis_map)
		
		fieldlist:=make([]string,0)
		for k,_ := range redis_map{
			fieldlist=append(fieldlist,k)
		}
		
		doc_id++

		this.Logger.Info("DOC_ID : %v  VALUE : %v", doc_id, writeCols)

	}

	for index, fields := range this.Fields {

		if this.Fields[index].IsIvt {

			utils.WriteToJson(fields.IvtIdx, fmt.Sprintf("./index/%v_idx.json", fields.Name))
			if this.Fields[index].FType == "T" {

				utils.WriteToJson(fields.IvtStrDic, fmt.Sprintf("./index/%v_dic.json", fields.Name))

			}

			if this.Fields[index].FType == "N" {

				utils.WriteToJson(fields.IvtNumDic, fmt.Sprintf("./index/%v_dic.json", fields.Name))
			}

		}

		if this.Fields[index].IsPlf {

			if this.Fields[index].FType == "T" {

				utils.WriteToJson(fields.PlfText, fmt.Sprintf("./index/%v_pfl.json", fields.Name))

			}

			if this.Fields[index].FType == "N" {

				utils.WriteToJson(fields.PlfNumber, fmt.Sprintf("./index/%v_pfl.json", fields.Name))

			}

		}

	}

	return nil

}
