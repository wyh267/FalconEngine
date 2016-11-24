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
	"time"
	"unsafe"
	"utils"
)

const (
	eProcessorOnlySupportPost    string = "提交方式错误,请查看提交方式是否正确"
	eProcessoParms               string = "参数错误"
	eProcessoJsonParse           string = "JSON格式解析错误"
	eProcessoUpdateProcessorBusy string = "处理进程繁忙，请稍候提交"
	eProcessoQueryError          string = "查询条件有问题，请检查查询条件"
	eHasCidError                 string = "CID已经存在"
	eNoIndexname                 string = "Indexname不存在"
	eDefaultEngineNotFound       string = `{"status":"NotFound"}`
	eDefaultEngineLoadOk         string = `{"status":"OK"}`
	eDefaultEngineLoadFail       string = `{"status":"Fail"}`
)

type DefaultResult struct {
	CostTime   string              `json:"costTime"`
	PageSize   int64               `json:"pageSize"`
	PageNum    int64               `json:"pageNumber"`
	TotalCount int64               `json:"totalCount"`
	Status     string              `json:"status"`
	Result     []map[string]string `json:"dataDetail"`
}

type DefaultEngine struct {
	idxManagers map[string]*SemSearchMgt
	detail      *utils.BoltHelper
	mdetail     map[string]map[string]string
	Logger      *utils.Log4FE `json:"-"`
}

func NewDefaultEngine(logger *utils.Log4FE) *DefaultEngine {
	this := &DefaultEngine{Logger: logger, idxManagers: make(map[string]*SemSearchMgt), mdetail: make(map[string]map[string]string)}
	this.detail = utils.NewBoltHelper(fmt.Sprintf("%v/%v.dtl", utils.IDX_ROOT_PATH, "Detail"), 666, logger)
	if this.detail == nil {
		return nil
	}

	if _, err := this.detail.CreateTable(IAccount); err != nil {
		this.Logger.Error("[ERROR] Create Table[%v] Error", IAccount)
		return nil
	}
	this.Logger.Info("[INFO] Create Table[%v] OK", IAccount)
	this.mdetail[IAccount] = make(map[string]string)

	if _, err := this.detail.CreateTable(ICampaign); err != nil {
		this.Logger.Error("[ERROR] Create Table[%v] Error", ICampaign)
		return nil
	}
	this.Logger.Info("[INFO] Create Table[%v] OK", ICampaign)
	this.mdetail[ICampaign] = make(map[string]string)

	if _, err := this.detail.CreateTable(IAdgroup); err != nil {
		this.Logger.Error("[ERROR] Create Table[%v] Error", IAdgroup)
		return nil
	}
	this.Logger.Info("[INFO] Create Table[%v] OK", IAdgroup)
	this.mdetail[IAdgroup] = make(map[string]string)

	if _, err := this.detail.CreateTable(IKeyword); err != nil {
		this.Logger.Error("[ERROR] Create Table[%v] Error", IKeyword)
		return nil
	}
	this.Logger.Info("[INFO] Create Table[%v] OK", IKeyword)
	this.mdetail[IKeyword] = make(map[string]string)

	if _, err := this.detail.CreateTable(ICreative); err != nil {
		this.Logger.Error("[ERROR] Create Table[%v] Error", ICreative)
		return nil
	}
	this.Logger.Info("[INFO] Create Table[%v] OK", ICreative)
	this.mdetail[ICreative] = make(map[string]string)
	return this
}

func (this *DefaultEngine) Search(method string, parms map[string]string, body []byte) (string, error) {

	//this.Logger.Info("[INFO] DefaultEngine Search >>>>>>>>")

	startTime := time.Now()
	cid, hascid := parms["cid"]
	accountid, hasaccountid := parms["account_id"]
	adgroupid, hasadgroupid := parms["adgroup_id"]
	campaignid, hascampaignid := parms["campaign_id"]
	keywordid, haskeywordid := parms["keyword_id"]

	indexname, hasindex := parms["index"]
	keyword, haskeyword := parms["keyword"]

	creativetitle, hastitle := parms["creativetitle"]
	creativedesc1, hasdesc1 := parms["creativedesc1"]
	creativedesc2, hasdesc2 := parms["creativedesc2"]

	matchtype, hasmatchtype := parms["matchtype"]

	ps, hasps := parms["ps"]
	pg, haspg := parms["pg"]

	if !hascid || !hasindex || !haspg || !hasps {
		return "", errors.New(eProcessoParms)
	}

	searchquerys := make([]utils.FSSearchQuery, 0)
	searchfilted := make([]utils.FSSearchFilted, 0)

	switch indexname {
	case IKeyword:
		if haskeyword && hasmatchtype {
			terms := utils.GSegmenter.SegmentSingle(keyword)
			if len(terms) == 0 {
				return eDefaultEngineNotFound, nil
			}
			//this.Logger.Info("[INFO] SegmentTerms >>>  %v ", terms)
			for _, term := range terms {
				var queryst utils.FSSearchQuery
				queryst.FieldName = "media_keyword"
				queryst.Value = term
				searchquerys = append(searchquerys, queryst)
			}
			switch matchtype {
			case "prefix":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_keyword", MatchStr: keyword, Type: utils.FILT_STR_PREFIX})
			case "suffix":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_keyword", MatchStr: keyword, Type: utils.FILT_STR_SUFFIX})
			case "range":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_keyword", MatchStr: keyword, Type: utils.FILT_STR_RANGE})
			case "all":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_keyword", MatchStr: keyword, Type: utils.FILT_STR_ALL})

			default:
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_keyword", MatchStr: keyword, Type: utils.FILT_STR_PREFIX})

			}

		}

	case ICreative:
		if hastitle && hasmatchtype {
			terms := utils.GSegmenter.SegmentSingle(creativetitle)
			if len(terms) == 0 {
				return eDefaultEngineNotFound, nil
			}
			for _, term := range terms {
				var queryst utils.FSSearchQuery
				queryst.FieldName = "media_creative_title"
				queryst.Value = term
				searchquerys = append(searchquerys, queryst)
			}

			switch matchtype {
			case "prefix":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_title", MatchStr: creativetitle, Type: utils.FILT_STR_PREFIX})
			case "suffix":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_title", MatchStr: creativetitle, Type: utils.FILT_STR_SUFFIX})
			case "range":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_title", MatchStr: creativetitle, Type: utils.FILT_STR_RANGE})
			case "all":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_title", MatchStr: creativetitle, Type: utils.FILT_STR_ALL})

			default:
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_title", MatchStr: creativetitle, Type: utils.FILT_STR_PREFIX})

			}

		}
		if hasdesc1 && hasmatchtype {
			terms := utils.GSegmenter.SegmentSingle(creativedesc1)
			if len(terms) == 0 {
				return eDefaultEngineNotFound, nil
			}
			for _, term := range terms {
				var queryst utils.FSSearchQuery
				queryst.FieldName = "media_creative_description1"
				queryst.Value = term
				searchquerys = append(searchquerys, queryst)
			}

			switch matchtype {
			case "prefix":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_description1", MatchStr: creativedesc1, Type: utils.FILT_STR_PREFIX})
			case "suffix":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_description1", MatchStr: creativedesc1, Type: utils.FILT_STR_SUFFIX})
			case "range":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_description1", MatchStr: creativedesc1, Type: utils.FILT_STR_RANGE})
			case "all":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_description1", MatchStr: creativedesc1, Type: utils.FILT_STR_ALL})

			default:
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_description1", MatchStr: creativedesc1, Type: utils.FILT_STR_PREFIX})

			}
		}
		if hasdesc2 && hasmatchtype {
			terms := utils.GSegmenter.SegmentSingle(creativedesc2)
			if len(terms) == 0 {
				return eDefaultEngineNotFound, nil
			}
			for _, term := range terms {
				var queryst utils.FSSearchQuery
				queryst.FieldName = "media_creative_description2"
				queryst.Value = term
				searchquerys = append(searchquerys, queryst)
			}

			switch matchtype {
			case "prefix":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_description2", MatchStr: creativedesc2, Type: utils.FILT_STR_PREFIX})
			case "suffix":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_description2", MatchStr: creativedesc2, Type: utils.FILT_STR_SUFFIX})
			case "range":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_description2", MatchStr: creativedesc2, Type: utils.FILT_STR_RANGE})
			case "all":
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_description2", MatchStr: creativedesc2, Type: utils.FILT_STR_ALL})

			default:
				searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_creative_description2", MatchStr: creativedesc2, Type: utils.FILT_STR_PREFIX})

			}

		}
	default:
		return "indexname错误", nil

	}

	if hasaccountid {
		if accid, err := strconv.ParseInt(accountid, 0, 0); err == nil {
			searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "account_id", Start: accid, Type: utils.FILT_EQ})
		}
	}

	if hascampaignid {
		if campid, err := strconv.ParseInt(campaignid, 0, 0); err == nil {
			searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_campaign_id", Start: campid, Type: utils.FILT_EQ})
		}
	}

	if hasadgroupid {
		if adid, err := strconv.ParseInt(adgroupid, 0, 0); err == nil {
			searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_adgroup_id", Start: adid, Type: utils.FILT_EQ})
		}
	}

	if haskeywordid {
		if kid, err := strconv.ParseInt(keywordid, 0, 0); err == nil {
			searchfilted = append(searchfilted, utils.FSSearchFilted{FieldName: "media_keyword_id", Start: kid, Type: utils.FILT_EQ})
		}
	}

	nps, ok1 := strconv.ParseInt(ps, 0, 0)
	npg, ok2 := strconv.ParseInt(pg, 0, 0)
	if ok1 != nil || ok2 != nil {
		nps = 10
		npg = 1
	}

	if nps <= 0 {
		nps = 10
	}

	if npg <= 0 {
		npg = 1
	}

	if _, ok := this.idxManagers[cid]; !ok {

		this.idxManagers[cid] = newSemSearchMgt(cid, this.Logger)
		if this.idxManagers[cid] == nil {
			return fmt.Sprintf("Cid[%v] Create Error", cid), nil
		}
	}

	indexer := this.idxManagers[cid].GetIndex(indexname)
	//this.Logger.Info("[INFO] searchquerys %v", searchquerys)
	//this.queryAnalyse(searchunit)
	//this.Logger.Info("[INFO] searchfilted %v", searchfilted)

	docids, found := indexer.SearchDocIds(searchquerys, searchfilted)

	if !found {
		return eDefaultEngineNotFound, nil
	}

	lens := int64(len(docids))

	start := nps * (npg - 1)
	end := nps * npg

	if start >= lens {
		return eDefaultEngineNotFound, nil
	}

	if end >= lens {
		end = lens
	}

	var defaultResult DefaultResult

	defaultResult.Result = make([]map[string]string, 0)
	for _, docid := range docids[start:end] {
		val, ok := indexer.GetDocument(docid.Docid)
		if ok {
			//for _, term := range terms {
			//	val["title"] = strings.Replace(val["title"], term, "[["+term+"]]", -1)
			//}
			defaultResult.Result = append(defaultResult.Result, val)
		}
	}
	utils.GiveDocIDsChan <- docids

	endTime := time.Now()
	defaultResult.CostTime = fmt.Sprintf("%v", endTime.Sub(startTime))
	defaultResult.PageNum = npg
	defaultResult.PageSize = nps
	defaultResult.Status = "Found"
	defaultResult.TotalCount = lens

	r, err := json.Marshal(defaultResult)
	if err != nil {
		return eDefaultEngineNotFound, err
	}

	bh := (*reflect.SliceHeader)(unsafe.Pointer(&r))
	sh := reflect.StringHeader{bh.Data, bh.Len}
	return *(*string)(unsafe.Pointer(&sh)), nil

}

func (this *DefaultEngine) CreateIndex(method string, parms map[string]string, body []byte) error {

	cid, hascid := parms["cid"]

	if !hascid {
		return errors.New(eProcessoParms)
	}

	if _, ok := this.idxManagers[cid]; ok {
		return errors.New(eHasCidError)
	}

	this.idxManagers[cid] = newSemSearchMgt(cid, this.Logger)
	if this.idxManagers[cid] == nil {
		return fmt.Errorf("Cid[%v] Create Error", cid)
	}

	return nil

}

func (this *DefaultEngine) UpdateDocument(method string, parms map[string]string, body []byte) (string, error) {
	cid, hascid := parms["cid"]

	if !hascid || method != "POST" {
		return "", errors.New(eProcessoParms)
	}

	document := make(map[string]string)
	err := json.Unmarshal(body, &document)
	if err != nil {
		this.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
		return "", errors.New(eProcessoJsonParse)
	}

	indexname, hasindexname := document["indexname"]
	if !hasindexname {
		return "", errors.New(eNoIndexname)
	}

	if _, ok := this.idxManagers[cid]; !ok {
		this.idxManagers[cid] = newSemSearchMgt(cid, this.Logger)
		if this.idxManagers[cid] == nil {
			return "", fmt.Errorf("Cid[%v] Create Error", cid)
		}
	}

	return this.idxManagers[cid].updateDocument(indexname, document)
}

func (this *DefaultEngine) LoadData(method string, parms map[string]string, body []byte) (string, error) {

	//cid, hascid := parms["cid"]
	var indexname string
	var hasindexname bool

	if /*!hascid ||*/ method != "POST" {
		return eDefaultEngineLoadFail, errors.New(eProcessoParms)
	}

	idxCount := make(map[string]int)
	idxCount["adgroup"] = 0
	idxCount["account"] = 0
	idxCount["campaign"] = 0
	idxCount["keyword"] = 0
	idxCount["creative"] = 0

	var loadstruct utils.FSLoadStruct
	err := json.Unmarshal(body, &loadstruct)
	if err != nil {
		this.Logger.Error("[ERROR] Parse JSON Fail : %v ", err)
		return eDefaultEngineLoadFail, errors.New(eProcessoJsonParse)
	}

	this.Logger.Info("[INFO] loadstruct %v %v", loadstruct, string(body))

	datafile, err := os.Open(loadstruct.Filename)
	if err != nil {
		this.Logger.Error("[ERROR] Open File[%v] Error %v", loadstruct.Filename, err)

		return eDefaultEngineLoadFail, errors.New("[ERROR] Open File Error")
	}
	defer datafile.Close()

	scanner := bufio.NewScanner(datafile)
	//i := 0
	var isJson bool
	if loadstruct.Split == "json" {
		isJson = true
	}

	if loadstruct.SyncCount <= 0 {
		loadstruct.SyncCount = 1000
	}
	rcount := 0
	for scanner.Scan() {
		content := make(map[string]string)
		var textcontent string
		if isJson {
			textcontent = scanner.Text()
			if err := json.Unmarshal([]byte(textcontent), &content); err != nil {
				//this.Logger.Error("[ERROR]  %v", err)
				this.Logger.Error("[INFO]  %v \t %v ", err, scanner.Text())
				continue
			}

		} else {
			return "", errors.New(eProcessoParms)
		}

		cid, hascid := content["cid"]

		if !hascid {
			this.Logger.Error("[ERROR]  %v", eProcessoParms)
			continue
		}

		if _, ok := this.idxManagers[cid]; !ok {
			this.idxManagers[cid] = newSemSearchMgt(cid, this.Logger)
			if this.idxManagers[cid] == nil {
				return "", fmt.Errorf("Cid[%v] Create Error", cid)
			}
		}

		indexname, hasindexname = content["indexname"]
		if !hasindexname {
			return "", errors.New(eNoIndexname)
		}

		accid, ok := content["account_id"]
		if !ok {
			return "", errors.New("account_id不存在")
		}

		detailkey := accid

		switch indexname {
		case IKeyword:
			if kid, ok := content["media_keyword_id"]; ok {
				detailkey = fmt.Sprintf("%v.%v", accid, kid)
			} else {
				detailkey = ""
			}
		case ICreative:
			if kid, ok := content["media_creative_id"]; ok {
				detailkey = fmt.Sprintf("%v.%v", accid, kid)
			} else {
				detailkey = ""
			}
		case IAccount:

			detailkey = content["account_id"]

		case ICampaign:
			if kid, ok := content["media_campaign_id"]; ok {
				detailkey = fmt.Sprintf("%v.%v", accid, kid)
			} else {
				detailkey = ""
			}
		case IAdgroup:
			if kid, ok := content["media_adgroup_id"]; ok {
				detailkey = fmt.Sprintf("%v.%v", accid, kid)
			} else {
				detailkey = ""
			}
		default:
			detailkey = ""
		}

		this.idxManagers[cid].addDocument(indexname, content)
		//this.idxManagers[cid].updateDocument(indexname, content)
		idxCount[indexname] = idxCount[indexname] + 1

		if idxCount[indexname]%loadstruct.SyncCount == 0 {

			for cid, _ := range this.idxManagers {
				this.idxManagers[cid].sync(indexname)
			}
		}
		rcount++
		if rcount%10000 == 0 {
			this.Logger.Info("[INFO] Read Data [ %v ] ", rcount)
			this.syncDetail()
		}
		//fmt.Println(sptext)
		//更新
		this.updateDetail(indexname, detailkey, textcontent)

	}
	this.syncDetail()
	for c, _ := range this.idxManagers {
		this.idxManagers[c].syncAll()
		if loadstruct.IsMerge {
			this.idxManagers[c].mergeAll()
		}

	}

	return eDefaultEngineLoadOk, nil

}

func (this *DefaultEngine) updateDetail(indexname, key, value string) error {

	this.mdetail[indexname][key] = value

	return nil

}

func (this *DefaultEngine) syncDetail() error {

	this.Logger.Info("[INFO] sync detail ")
	for indexname, val := range this.mdetail {
		this.detail.UpdateMuti(indexname, val)
	}
	this.mdetail = make(map[string]map[string]string)
	this.mdetail[IAccount] = make(map[string]string)
	this.mdetail[ICampaign] = make(map[string]string)
	this.mdetail[IAdgroup] = make(map[string]string)
	this.mdetail[IKeyword] = make(map[string]string)
	this.mdetail[ICreative] = make(map[string]string)
	return nil
}
