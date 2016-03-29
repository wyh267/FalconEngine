/*****************************************************************************
 *  file name : defaultEngine.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数据层之上的引擎层
 *
******************************************************************************/

package FalconEngine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
	"utils"
)

const (
	eProcessorOnlySupportPost    string = "提交方式错误,请查看提交方式是否正确"
	eProcessoParms               string = "参数错误"
	eProcessoJsonParse           string = "JSON格式解析错误"
	eProcessoUpdateProcessorBusy string = "处理进程繁忙，请稍候提交"
	eProcessoQueryError          string = "查询条件有问题，请检查查询条件"
)

type DefaultEngine struct {
	idxManager *IndexMgt
	Logger     *utils.Log4FE `json:"-"`
}

func NewDefaultEngine(logger *utils.Log4FE) *DefaultEngine {
	this := &DefaultEngine{Logger: logger, idxManager: newIndexMgt(logger)}
	return this
}

func (this *DefaultEngine) Search(method string, parms map[string]string, body []byte) (string, error) {

	//this.Logger.Info("[INFO] DefaultEngine Search >>>>>>>>")
	indexname, hasindex := parms["index"]
	query, hasquery := parms["q"]
	ps, hasps := parms["ps"]
	pg, haspg := parms["pg"]

	if !hasindex || !hasquery || !haspg || !hasps {
		return "", errors.New(eProcessoParms)
	}

    terms:= utils.GSegmenter.Segment(query,false)
    if len(terms) == 0 {
        return "not found", nil
    }
    
    searchquerys := make([]utils.FSSearchQuery,0)
    for _,term:=range terms{
        var queryst utils.FSSearchQuery
	    queryst.FieldName = "content"
	    queryst.Value = term
        searchquerys=append(searchquerys,queryst)
    }

	nps, ok := strconv.ParseInt(ps, 0, 0)
	npg, ok := strconv.ParseInt(pg, 0, 0)
	if ok != nil {
		return "", errors.New(eProcessoParms)
	}

	//this.queryAnalyse(searchunit)

	res, ok1 := this.idxManager.Search(indexname,searchquerys , nil, int(nps), int(npg))
	if !ok1 {
		return "not found", nil
	}

	r, err := json.Marshal(res)
	if err != nil {
		return "", err
	}

	bh := (*reflect.SliceHeader)(unsafe.Pointer(&r))
    sh := reflect.StringHeader{bh.Data, bh.Len}
    return *(*string)(unsafe.Pointer(&sh)),nil

	//return string(r), nil

}

func (this *DefaultEngine) CreateIndex(method string, parms map[string]string, body []byte) error {

	indexname, hasindex := parms["index"]

	if !hasindex {
		return errors.New(eProcessoParms)
	}

	var indexstruct utils.IndexStrct
	err := json.Unmarshal(body, &indexstruct)
	if err != nil {
		this.Logger.Error("[ERROR]  %v : %v ", eProcessoJsonParse, err)
		return fmt.Errorf("[ERROR]  %v : %v ", eProcessoJsonParse, err)
	}

	return this.idxManager.CreateIndex(indexname, indexstruct.IndexMapping)

}

func (this *DefaultEngine) CreateEmptyIndex(indexname string) error {

	return this.idxManager.CreateEmptyIndex(indexname)

}

func (this *DefaultEngine) AddField(indexname string, field utils.SimpleFieldInfo) error {

	return this.idxManager.AddField(indexname, field)

}

func (this *DefaultEngine) queryAnalyse(query utils.FSSearchFrontend) utils.FSSearchUnit {

	var unit utils.FSSearchUnit

	return unit

}

func (this *DefaultEngine) UpdateDocument(method string, parms map[string]string, body []byte) (string, error) {
	indexname, hasindex := parms["index"]

	if !hasindex || method != "POST" {
		return "", errors.New(eProcessoParms)
	}

	document := make(map[string]string)
	err := json.Unmarshal(body, &document)
	if err != nil {
		this.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
		return "", errors.New(eProcessoJsonParse)
	}

	return this.idxManager.updateDocument(indexname, document)
}

func (this *DefaultEngine) LoadData(method string, parms map[string]string, body []byte) (string, error) {

	indexname, hasindex := parms["index"]

	if !hasindex || method != "POST" {
		return "", errors.New(eProcessoParms)
	}

	var loadstruct utils.FSLoadStruct
	err := json.Unmarshal(body, &loadstruct)
	if err != nil {
		this.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
		return "", errors.New(eProcessoJsonParse)
	}

	this.Logger.Info("[INFO] loadstruct %v %v", loadstruct, string(body))

	datafile, err := os.Open(loadstruct.Filename)
	if err != nil {
		this.Logger.Error("[ERROR] Open File[%v] Error %v", loadstruct.Filename, err)

		return "", errors.New("[ERROR] Open File Error")
	}
	defer datafile.Close()

	scanner := bufio.NewScanner(datafile)
	i := 0
	for scanner.Scan() {
		sptext := strings.Split(scanner.Text(), loadstruct.Split)
		content := make(map[string]string)
		if len(sptext) != len(loadstruct.Fields) {
			continue
		}
		for idx, fname := range loadstruct.Fields {
			content[fname] = sptext[idx]
		}

		this.idxManager.updateDocument(indexname, content)

		i++
		if i%100000 == 0 {
			this.idxManager.sync(indexname)
		}
		//fmt.Println(sptext)
	}
	this.idxManager.sync(indexname)

	return "", this.idxManager.mergeIndex(indexname)

}
