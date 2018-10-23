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
	fi "FalconIndex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
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
	idxManagers map[string]*fi.Index
	idxNodes    map[string]utils.NodeIndex
	Logger      *utils.Log4FE `json:"-"`
	LocalIP     string
	MasterIP    string
	MasterPort  int
	LocalPort   int
}

func NewDefaultEngine(localip, masterip string, localport, masterport int, logger *utils.Log4FE) *DefaultEngine {
	this := &DefaultEngine{idxNodes: make(map[string]utils.NodeIndex), LocalPort: localport, MasterPort: masterport, LocalIP: localip, MasterIP: masterip, Logger: logger, idxManagers: make(map[string]*fi.Index)}

	return this
}

// Search function description : 搜索
// params :
// return :
func (this *DefaultEngine) Search(method string, parms map[string]string, body []byte) (string, error) {

	//this.Logger.Info("[INFO] DefaultEngine Search >>>>>>>>")

	startTime := time.Now()
	indexname, hasindex := parms["index"]
	ps, hasps := parms["ps"]
	pg, haspg := parms["pg"]
	req, _ := parms["_req"]
	_, hasforce := parms["_force"]
	shardn, hasshard := parms["_shard"]

	if !hasindex || !haspg || !hasps {
		return "", errors.New(eProcessoParms)
	}

	searchquerys := make([]utils.FSSearchQuery, 0)
	searchfilted := make([]utils.FSSearchFilted, 0)

	for field, value := range parms {
		if field == "cid" || field == "index" || field == "ps" || field == "pg" || field == "_shard" || field == "_force" || field == "_req" {
			continue
		}

		switch field[0] {
		case '-': //正向过滤
			value_list := strings.Split(value, ",")
			sf := utils.FSSearchFilted{FieldName: field[1:], Type: utils.FILT_EQ, Range: make([]int64, 0)}
			for _, v := range value_list {

				if valuenum, err := strconv.ParseInt(v, 0, 0); err == nil {
					sf.Range = append(sf.Range, valuenum)
				}
			}
			searchfilted = append(searchfilted, sf)

		case '_': //反向过滤
			value_list := strings.Split(value, ",")
			sf := utils.FSSearchFilted{FieldName: field[1:], Type: utils.FILT_NOT, Range: make([]int64, 0)}
			for _, v := range value_list {

				if valuenum, err := strconv.ParseInt(v, 0, 0); err == nil {
					sf.Range = append(sf.Range, valuenum)
				}
			}
			searchfilted = append(searchfilted, sf)

		default: //搜索
			//sf := utils.FSSearchFilted{FieldName: field, Type: utils.FILT_STR_PREFIX, RangeStr: make([]string, 0)}
			terms := utils.GSegmenter.Segment(value, true)
			if len(terms) == 0 {
				return eDefaultEngineNotFound, nil
			}
			//this.Logger.Info("[INFO] SegmentTerms >>>  %v ", terms)
			for _, term := range terms {
				var queryst utils.FSSearchQuery
				queryst.FieldName = field
				queryst.Value = term
				searchquerys = append(searchquerys, queryst)
			}

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

	var defaultResult DefaultResult
	var lens int64
	if hasforce && hasshard {
		idxname := fmt.Sprintf("%v_%v", indexname, shardn)
		docids, _ := this.idxManagers[idxname].SearchDocIds(searchquerys, searchfilted)
		this.Logger.Info("[INFO] RES FORCE LOCAL ::: %v", docids)
		lens = int64(len(docids))
		start := nps * (npg - 1)
		end := nps * npg
		if start >= lens {
			return eDefaultEngineNotFound, nil
		}
		if end >= lens {
			end = lens
		}

		defaultResult.Result = make([]map[string]string, 0)
		for _, docid := range docids[start:end] {
			val, ok := this.idxManagers[idxname].GetDocument(docid.Docid)
			if ok {
				defaultResult.Result = append(defaultResult.Result, val)
			}
		}

		utils.GiveDocIDsChan <- docids
	} else {
		//获取索引的分片
		if idxnode, ok := this.idxNodes[indexname]; ok {

			for shard := uint64(0); shard < idxnode.ShardNum; shard++ {
				flag := false
				for _, s := range idxnode.Shard {
					if s == shard {
						//indexer.SearchDocIds(searchquerys, searchfilted)
						idxname := fmt.Sprintf("%v_%v", indexname, shard)
						docids, _ := this.idxManagers[idxname].SearchDocIds(searchquerys, searchfilted)

						this.Logger.Info("[INFO] RES LOCAL ::: %v", docids)

						lens = int64(len(docids))
						start := nps * (npg - 1)
						end := nps * npg
						if start >= lens {
							return eDefaultEngineNotFound, nil
						}
						if end >= lens {
							end = lens
						}

						defaultResult.Result = make([]map[string]string, 0)
						for _, docid := range docids[start:end] {
							val, ok := this.idxManagers[idxname].GetDocument(docid.Docid)
							if ok {
								defaultResult.Result = append(defaultResult.Result, val)
							}
						}

						utils.GiveDocIDsChan <- docids

						flag = true
						break
					}

				}

				if !flag {
					addr := idxnode.ShardNodes[shard][0]
					url := fmt.Sprintf("http://%v%v&_force=1&_shard=%v", addr, req, shard)
					this.Logger.Info("[INFO] Req  %v", url)
					res, err := utils.RequestUrl(url)
					if err != nil {
						this.Logger.Error("[ERROR] error %v", err)
						continue
					}
					this.Logger.Info("[INFO] RES:: %v", string(res))
				}

			}

		} else {
			return eNoIndexname, nil
		}
	}

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

	/*
		indexer := this.idxManagers[indexname]
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

				accid, ok := val["account_id"]
				if !ok {
					this.Logger.Error("[ERROR]  account_id不存在")
					continue
				}

				if accstr, err1 := this.detail.Get(IAccount, fmt.Sprintf("%v.%v", cid, accid)); err1 == nil {

					var v map[string]string
					err := json.Unmarshal([]byte(accstr), &v)
					if err != nil {
						this.Logger.Error("[ERROR] json err %v", err)
					} else {
						val["media_username"] = v["media_username"]
					}

				}

				capid, hcapid := val["media_campaign_id"]
				adgid, hadgid := val["media_adgroup_id"]

				if (indexname == IKeyword || indexname == ICreative) && hadgid && hcapid {
					if capstr, err1 := this.detail.Get(ICampaign, fmt.Sprintf("%v.%v.%v", cid, accid, capid)); err1 == nil {
						if adgstr, err2 := this.detail.Get(IAdgroup, fmt.Sprintf("%v.%v.%v.%v", cid, accid, capid, adgid)); err2 == nil {
							var v map[string]string
							err := json.Unmarshal([]byte(adgstr), &v)
							if err != nil {
								this.Logger.Error("[ERROR] json err %v", err)
							} else {
								val["media_adgroup_name"] = v["media_adgroup_name"]
							}

						}

						var v map[string]string
						err := json.Unmarshal([]byte(capstr), &v)
						if err != nil {
							this.Logger.Error("[ERROR] json err %v", err)
						} else {
							val["media_campaign_name"] = v["media_campaign_name"]
						}

					}

				}

				if (indexname == IAdgroup) && hcapid {
					if capstr, err1 := this.detail.Get(ICampaign, fmt.Sprintf("%v.%v.%v", cid, accid, capid)); err1 == nil {

						var v map[string]string
						err := json.Unmarshal([]byte(capstr), &v)
						if err != nil {
							this.Logger.Error("[ERROR] json err %v", err)
						} else {
							val["media_campaign_name"] = v["media_campaign_name"]
						}

					}

				}

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
	*/
	return "", nil

}

func (this *DefaultEngine) CreateIndex(method string, parms map[string]string, body []byte) error {

	return nil

}

func (this *DefaultEngine) UpdateDocument(method string, parms map[string]string, body []byte) (string, error) {
	/*
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
	*/
	return "", nil
}

func (this *DefaultEngine) LoadData(method string, parms map[string]string, body []byte) (string, error) {

	return eDefaultEngineLoadOk, nil

}

func (this *DefaultEngine) PullDetail(method string, parms map[string]string, body []byte) ([]string, uint64) {
	return nil, 0
}

func (this *DefaultEngine) JoinNode(method string, parms map[string]string, body []byte) (string, error) {

	return "nil", nil
}

func (this *DefaultEngine) Heart(method string, parms map[string]string, body []byte) (map[string]string, error) {

	dtype, hastype := parms["type"]

	res := make(map[string]string)
	if !hastype {

		return nil, fmt.Errorf("parms error")
	}

	switch dtype {
	case "alive":
		res["status"] = "alive"
	case "addindex":
		this.Logger.Info("[INFO] body %v", string(body))
		var nodeindex utils.NodeIndex
		if err := json.Unmarshal(body, &nodeindex); err != nil {
			this.Logger.Error("[ERROR] nodeindex json error %v", err)
		}
		this.Logger.Info("[INFO] nodeindex %v", nodeindex)
		nodeindex.Shard = make([]uint64, 0)
		local := fmt.Sprintf("%v:%v", this.LocalIP, this.LocalPort)
		for k, v := range nodeindex.ShardNodes {
			for _, s := range v {
				if local == s {
					indexname := fmt.Sprintf("%v_%v", nodeindex.IndexName, k)
					err := this.createIndex(indexname, utils.IDX_ROOT_PATH, nodeindex.IndexMapping)
					if err != nil {
						this.Logger.Error("[ERROR] create index error  %v", err)
						res["status"] = err.Error()
						return res, nil
					}
					nodeindex.Shard = append(nodeindex.Shard, uint64(k))

					go this.pullDetail(nodeindex.IndexName, k)

				}
			}
		}
		this.idxNodes[nodeindex.IndexName] = nodeindex
		res["status"] = "add index Success"

	default:
		res["status"] = "alive"
	}

	return res, nil
}

type DetailRes struct {
	MaxId     uint64   `json:"_maxid"`
	Detail    []string `json:"_data"`
	ErrorCode int64    `json:"_errorcode"`
	Status    string   `json:"_status"`
}

func (this *DefaultEngine) addDocument(indexname string, document map[string]string) (string, error) {

	if _, ok := this.idxManagers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return "", fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}
	_, err := this.idxManagers[indexname].UpdateDocument(document, utils.UPDATE_TYPE_ADD)
	return "{ \"status\":\"OK\" }", err
}

func (this *DefaultEngine) pullDetail(indexname string, shardnum uint64) error {
	//127.0.0.1:9990/v1/_pull?index=testidx&shardnum=2&start=0&lens=100
	start := uint64(0)
	lens := 1000
	idxname := fmt.Sprintf("%v_%v", indexname, shardnum)
	pullcount := 0
	for {
		time.Sleep(time.Second * 5)
		url := fmt.Sprintf("http://%v:%v/v1/_pull?index=%v&shardnum=%v&start=%v&lens=%v", this.MasterIP, this.MasterPort, indexname, shardnum, start, lens)
		this.Logger.Info("[INFO] Pull Detail  %v", url)
		res, err := utils.RequestUrl(url)
		if err != nil {
			this.Logger.Error("[ERROR] error %v", err)
			continue
		} else {
			var detail DetailRes
			err := json.Unmarshal(res, &detail)
			if err != nil || detail.ErrorCode != 0 {
				this.Logger.Error("[ERROR] error %v  errorCode : %v ", err, detail.ErrorCode)
				if pullcount >= 3 {
					this.mergeIndex(idxname)
					pullcount = 0
				}

				continue
			}
			pullcount++
			//this.Logger.Info("[INFO] pull Detail : %v", detail)
			this.Logger.Info("[INFO] MaxId %v", detail.MaxId)
			start = detail.MaxId

			var document map[string]string
			for _, doc := range detail.Detail {

				err := json.Unmarshal([]byte(doc), &document)
				if err != nil {
					continue
				}
				this.addDocument(idxname, document)
			}
			this.sync(idxname)

		}
	}

}

func (this *DefaultEngine) sync(indexname string) error {

	if _, ok := this.idxManagers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.idxManagers[indexname].SyncMemorySegment()
}

func (this *DefaultEngine) mergeIndex(indexname string) error {

	if _, ok := this.idxManagers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.idxManagers[indexname].MergeSegments(-1)
}

func (this *DefaultEngine) createIndex(indexname, pathname string, fields []utils.SimpleFieldInfo) error {

	if _, ok := this.idxManagers[indexname]; ok {
		return errors.New(eHasCidError)
	}

	this.idxManagers[indexname] = fi.NewEmptyIndex(indexname, utils.IDX_ROOT_PATH, this.Logger)
	for _, field := range fields {
		this.Logger.Info("[INFO] field %v", field)
		this.idxManagers[indexname].AddField(field)
	}

	return nil

}

func (this *DefaultEngine) InitEngine() error {

	joinurl := fmt.Sprintf("http://%v:%v/v1/_join?addr=%v&mport=%v", this.MasterIP, this.MasterPort, this.LocalIP, this.LocalPort)

	this.Logger.Info("[INFO] url %v", joinurl)

	res, err := utils.RequestUrl(joinurl)
	if err != nil {
		this.Logger.Error("[ERROR] error %v", err)
	} else {
		this.Logger.Info("[INFO] RES %v", string(res))
	}

	return nil
}
