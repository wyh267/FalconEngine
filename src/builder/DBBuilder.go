package builder

import (
	"errors"
	"fmt"
	"indexer"
	"strconv"
	"strings"
	"utils"
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
func (this *DBBuilder) parseConfigure() error {
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

	this.parseConfigure()

	this.Buiding()

	//fmt.Println("Start Building Index",yy)
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
				this.Fields[index].IvtStrDic = utils.NewStringIdxDic(10000)

			}

			if v.FType == "N" {
				this.Fields[index].IvtIdx = utils.NewInvertIdx(utils.TYPE_NUM, v.Name)
				this.Fields[index].IvtNumDic = utils.NewNumberIdxDic(10000)

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
