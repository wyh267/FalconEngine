package builder

import (
	"fmt"
	"indexer"
	"strconv"
	"strings"
	"time"
	"utils"
	"encoding/json"
	"plugins"

	//"os"
)

type FieldInfo struct {
	IsPK      bool	`json:"is_pk"`
	IsIvt     bool	`json:"is_ivt"`
	IsPlf     bool	`json:"is_pro"`
	FType     string `json:"field_type"`
	Name      string `json:"name"`
	SType     int64  `json:"split"`
	IvtIdx    *utils.InvertIdx
	IvtStrDic *utils.StringIdxDic
	IvtNumDic *utils.NumberIdxDic
	PlfText   *indexer.TextProfile
	PlfNumber *indexer.NumberProfile
	PlfByte   *indexer.ByteProfile
	
	CustomeInter utils.CustomInterface
	
	
}

type DBBuilder struct {
	*Builder
	sql       string
	Fields    []FieldInfo
	DetailIdx *indexer.Detail
}

func NewDBBuilder(b *Builder) *DBBuilder {
	this := &DBBuilder{b, "", make([]FieldInfo, 0), nil}
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
	//this.Logger.Info("All Fields : %v ",fields)
	for k, v := range fields {
		var fi FieldInfo
		err = json.Unmarshal([]byte(v), &fi)
		if err != nil {
			fmt.Printf("Unmarshal Error ...\n")
			return err
		}
		fi.Name=k
/*
		l := strings.Split(v, ",")
		if len(l) != 5 {
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
		stype, err := strconv.ParseInt(l[4], 0, 0)
		if err != nil {
			return err
		}

		fi.SType = stype
*/

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
	Info       map[string]string
	UpdateType int
	ErrChan    chan error
	LogId	   string
}

func (this *DBBuilder) ScanInc(Data_chan chan UpdateInfo) error {

	this.Logger.Info("Start Inc Updating Now ..... ")

	curr_time := time.Now().Add(-10*time.Minute).Format("2006-01-02 15:04:05")
	var fields string
	isIvert := make(map[string]bool)
	for _, v := range this.Fields {
		//构造sql语句
		fields = fields + "," + v.Name
		isIvert[v.Name] = v.IsIvt
	}

	incSql, _ := this.Configure.GetIncSql()
	incField, _ := this.Configure.GetIncField()
	for {
		sql := fmt.Sprintf(incSql, fields[1:len(fields)], curr_time)
		//fmt.Printf("SQL :: %v \n", sql)
		rows, err := this.Dbadaptor.QueryFormat(sql)
		if err != nil {
			this.Logger.Error(" %v", err)
			return err
		}
		defer rows.Close()
		for rows.Next() {
			//isUpdate := false
			updateType := indexer.PlfUpdate
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

			if new_values["is_delete"] == "1" {
				updateType = indexer.Delete
			}
			upinfo := UpdateInfo{new_values, updateType, make(chan error),"NO_LOG_ID"}
			Data_chan <- upinfo
			errinfo := <-upinfo.ErrChan
			if errinfo != nil {
				this.Logger.Info("Update Fail.... %v ", errinfo)
			} else {
				this.Logger.Info("Update Success.... ")
			}
			curr_time = new_values[incField]
			
			/*	
			pk, err := strconv.ParseInt(new_values["id"], 0, 0)
			if err != nil {
				this.Logger.Error("parse error : %v", err)
				continue
			}
			doc_ids, ok := this.Index_set.SearchField(pk, "id")
			if !ok { //新增DOC_ID
				isUpdate = true
				updateType = indexer.IvtUpdate
			} else {

				redis_map, err := this.Index_set.Detail.GetDocInfo(doc_ids[0].DocId)
				if err != nil {
					this.Logger.Error("Read Detail error...%v", err)
					continue
				}

				for k, v := range redis_map {
					vv, ok := new_values[k]
					if !ok {
						break
					}

					//this.Logger.Info("K : %v ==== V : %v === VV : %v",k,v,vv)
					if (v != vv) && k != incField {
						isUpdate = true

						//curr_time = new_values[incField]
						if isIvert[k] {
							updateType = indexer.IvtUpdate
						}
					}
				}

			}

			if isUpdate {
				curr_time = new_values[incField]
				if new_values["is_delete"] == "1" {
					updateType = indexer.Delete
				}
				this.Logger.Info("Update Status : Just Update Profile : [%v] ", updateType)
				upinfo := UpdateInfo{new_values, updateType, make(chan error)}
				Data_chan <- upinfo
				errinfo := <-upinfo.ErrChan
				if errinfo != nil {
					this.Logger.Info("Update Fail.... %v ", errinfo)
				} else {
					this.Logger.Info("Update Success.... ")
				}
			}
			*/

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
		
		//新建插件
		cumstom := plugins.NewPlus(v.Name)
		cumstom.Init()

		//构造索引数据指针
		if v.IsIvt {
			if v.FType == "T" {
				this.Fields[index].IvtIdx = utils.NewInvertIdx(utils.TYPE_TEXT, v.Name)
				this.Fields[index].IvtStrDic = utils.NewStringIdxDic(v.Name)
				this.Fields[index].CustomeInter=cumstom

			}

			if v.FType == "N" {
				
				this.Fields[index].IvtIdx = utils.NewInvertIdx(utils.TYPE_NUM, v.Name)
				this.Fields[index].IvtNumDic = utils.NewNumberIdxDic(v.Name)
				this.Fields[index].CustomeInter=cumstom
			}
			
			if v.FType == "I" {
				
				this.Fields[index].IvtIdx = utils.NewInvertIdx(utils.TYPE_TEXT, v.Name)
				this.Fields[index].IvtStrDic = utils.NewStringIdxDic(v.Name)
				this.Fields[index].CustomeInter=cumstom

			}
			
			
		}

		if v.IsPlf {
			if v.FType == "T" {
				this.Fields[index].PlfText = indexer.NewTextProfile(v.Name)
				this.Fields[index].PlfText.SetCustomInterface(cumstom)
			}

			if v.FType == "N" {
				this.Fields[index].PlfNumber = indexer.NewNumberProfile(v.Name)
				this.Fields[index].PlfNumber.SetCustomInterface(cumstom)
			}

			if v.FType == "I" {
				this.Fields[index].PlfByte = indexer.NewByteProfile(v.Name)
				this.Fields[index].PlfByte.SetCustomInterface(cumstom)
			}

		}

	}
	//fmt.Printf("%v\n", fields)

	this.DetailIdx = indexer.NewDetail()

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
	builder := &utils.IndexBuilder{Segmenter: segment, TempIndex: make(map[string][]utils.TmpIdx), TempIndexNum: make(map[string]int64)}

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

				if this.Fields[index].FType == "T" || this.Fields[index].FType == "I"{
					err := builder.BuildTextIndex(doc_id, v, this.Fields[index].IvtIdx, this.Fields[index].IvtStrDic, this.Fields[index].SType, false,this.Fields[index].CustomeInter)
					//err := builder.BuildTextIndexTemp(doc_id, v, this.Fields[index].IvtIdx, this.Fields[index].IvtStrDic,this.Fields[index].SType,this.Fields[index].Name)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
				}

				if this.Fields[index].FType == "N" {
					v_num, err := strconv.ParseInt(v, 0, 0)
					if err != nil {
						v_num = 0
						this.Logger.Warn("Warning : name : [%v] , value: [%v] , error : [%v]", this.Fields[index].Name, v, err)
					}

					err = builder.BuildNumberIndex(doc_id, v_num, this.Fields[index].IvtIdx, this.Fields[index].IvtNumDic, this.Fields[index].SType,false,this.Fields[index].CustomeInter)
					//err = builder.BuildNumberIndexTemp(doc_id, v_num, this.Fields[index].IvtIdx, this.Fields[index].IvtNumDic,this.Fields[index].Name)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
				}

			}

			if this.Fields[index].IsPlf {

				if this.Fields[index].FType == "T" {
					//添加日期类型的更新，仅精确到天 add by wuyinghao 2015-08-21
					if this.Fields[index].SType == 5 {
						vl := strings.Split(v, " ")
						v = vl[0]
					}else if this.Fields[index].SType == 9{
						v=this.Fields[index].PlfText.GetCustomInterface().BuildStringProfile(v)
					}
					err := this.Fields[index].PlfText.PutProfile(doc_id, v)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
				}

				if this.Fields[index].FType == "N" {
					v_num, err := strconv.ParseInt(v, 0, 0)
					if err != nil {
						v_num = 0
						this.Logger.Warn("Warning : name : %v , value: %v , error : %v", this.Fields[index].Name, v, err)
					}
					if this.Fields[index].SType == 9{
						v_num=this.Fields[index].PlfNumber.GetCustomInterface().BuildIntProfile(v)
					}
					err = this.Fields[index].PlfNumber.PutProfile(doc_id, v_num)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
				}

				if this.Fields[index].FType == "I" {
					v_byte := []byte(v)
					if this.Fields[index].SType == 9{
						v_byte=this.Fields[index].PlfByte.GetCustomInterface().BuildByteProfile(v_byte)
					}
					err = this.Fields[index].PlfByte.PutProfile(doc_id, v_byte)
					if err != nil {
						this.Logger.Error("ERROR : %v", err)
					}
				}

			}

		}
		/////
		//this.RedisCli.SetFields(doc_id, redis_map)
		////
		//for k,v := range redis_map{
		//	fmt.Printf("\"%v\":\"%v\",",k,v)
		//}
		//fmt.Printf("\n")
		if this.DetailIdx.PutDocInfo(doc_id, redis_map) != nil {
			this.Logger.Error("PutDocInfo doc_id Error :  %v \n", err)
		}

		fieldlist := make([]string, 0)
		for k, _ := range redis_map {
			fieldlist = append(fieldlist, k)
		}

		doc_id++
		if doc_id%5000 == 0 {
			this.Logger.Info("processing doc_id :  %v \n", doc_id)
		}
		//this.Logger.Info("DOC_ID : %v  VALUE : %v", doc_id, writeCols)

	}

	writeCount := 0
	writeChan := make(chan string, 1000)

	this.DetailIdx.WriteDetailToFile()
	this.Logger.Info("Make BitMap File")
	utils.MakeBitmapFile()

	for index, _ := range this.Fields {
		if this.Fields[index].IsIvt {
			//utils.WriteToIndexFile(this.Fields[index].IvtIdx, fmt.Sprintf("./index/%v_idx.idx", this.Fields[index].Name))
			this.Fields[index].IvtIdx.WriteToFile()
			if this.Fields[index].FType == "T" || this.Fields[index].FType == "I"{

				this.Fields[index].IvtStrDic.WriteToFile()

			}

			if this.Fields[index].FType == "N" {
				this.Fields[index].IvtNumDic.WriteToFile()
			}

		}

		if this.Fields[index].IsPlf {

			if this.Fields[index].FType == "T" {

				this.Fields[index].PlfText.WriteToFile()

			}

			if this.Fields[index].FType == "N" {

				this.Fields[index].PlfNumber.WriteToFile()

			}

			if this.Fields[index].FType == "I" {

				this.Fields[index].PlfByte.WriteToFile()
				//go this.Fields[index].PlfByte.WriteToFileWithChan(writeChan)
				//writeCount++

			}

		}

	}

	//fmt.Printf("Waiting %v threads\n ", writeCount)
	if writeCount == 0 {
		close(writeChan)
		return nil
	}
	for {
		select {
		case file_name := <-writeChan:
			writeCount--
			fmt.Printf("Write [%v] finished \n ", file_name)
			if writeCount == 0 {
				fmt.Printf("Finish building all index...\n")
				close(writeChan)
				return nil
			}
		}
	}

	return nil

}
