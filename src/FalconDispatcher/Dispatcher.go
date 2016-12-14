/*****************************************************************************
 *  file name : Dispatcher.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 分发中心，主节点
 *
******************************************************************************/

package FalconDispatcher

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
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

type ShardInfo struct {
	ShardPathName string      `json:"shardpathname"`
	OffsetMmap    *utils.Mmap `json:"-"`
	DetailMmap    *utils.Mmap `json:"-"`
	MaxOffset     uint64      `json:"maxoffset"`
	NodeInfos     []string    `json:"nodeinfos"`
}

type idxInfo struct {
	Name         string                  `json:"name"`
	Path         string                  `json:"path"`
	ShardNum     uint64                  `json:"shardnum"`
	MaxCnt       uint64                  `json:"maxCnt"`
	ShardField   string                  `json:"shardfield"`
	IndexMapping []utils.SimpleFieldInfo `json:"indexmapping"`
	ShardInfos   []ShardInfo             `json:"shardinfos"`
}

type Dispatcher struct {
	Logger    *utils.Log4FE
	NodeInfos []*utils.NodeNetInfo `json:"nodeinfos"`
	IndexInfo map[string]idxInfo   `json:"indexinfo"`
	LocalIP   string               `json:"localip"`
}

func NewDispatcher(localip string, logger *utils.Log4FE) *Dispatcher {
	this := &Dispatcher{Logger: logger, IndexInfo: make(map[string]idxInfo), NodeInfos: make([]*utils.NodeNetInfo, 0), LocalIP: localip}

	return this
}

// Search function description : 搜索
// params :
// return :
func (this *Dispatcher) Search(method string, parms map[string]string, body []byte) (string, error) {

	return "", nil
}

func (this *Dispatcher) JoinNode(method string, parms map[string]string, body []byte) (string, error) {

	addr, ok := parms["addr"]
	if !ok {
		return "nil", fmt.Errorf("parms error")
	}
	mport, hm := parms["mport"]
	if !hm {
		return "nil", fmt.Errorf("parms error")
	}

	//cport, hc := parms["cport"]
	//if !hc {
	//	return "nil", fmt.Errorf("parms error")
	//}
	/*
		res := make([]utils.NodeIndex, 0)
		this.Logger.Info("[INFO] this.IndexInfo %v", this.IndexInfo)
		for k, v := range this.IndexInfo {
			var ni utils.NodeIndex
			ni.IndexName = k
			ni.ShardNum = v.ShardNum
			ni.ShardNodes = make(map[uint64][]string)
			ni.Shard = make([]uint64, 0)
			for s, vv := range v.ShardInfos {

				if len(vv.NodeInfos) == 0 {
					this.IndexInfo[k].ShardInfos[s].NodeInfos = append(this.IndexInfo[k].ShardInfos[s].NodeInfos, addr)
					ni.Shard = append(ni.Shard, uint64(s))
				}
				ni.ShardNodes[uint64(s)] = this.IndexInfo[k].ShardInfos[s].NodeInfos
			}

			res = append(res, ni)

		}
	*/
	//res:=make(map[string])
	nd := utils.NodeNetInfo{Addr: addr, MPort: mport, CPort: "0", IdxChan: make(chan utils.NodeIndex, 5)}
	this.NodeInfos = append(this.NodeInfos, &nd)
	this.storeStruct()

	go this.controlThread(&nd)

	return "success", nil

}

func (this *Dispatcher) CreateIndex(method string, parms map[string]string, body []byte) error {

	var idxstruct utils.IndexStrct

	if err := json.Unmarshal(body, &idxstruct); err != nil {
		this.Logger.Error("[ERROR] json error  %v", err)
		return err
	}

	if _, ok := this.IndexInfo[idxstruct.IndexName]; ok {
		this.Logger.Error("[ERROR] index [%v] already has ", idxstruct.IndexName)
		return fmt.Errorf("[ERROR] index [%v] already has ", idxstruct.IndexName)
	}

	sis := make([]ShardInfo, 0)
	seeds := make([]bool, len(this.NodeInfos))
	if len(this.NodeInfos) < int(idxstruct.ShardNum) {
		this.Logger.Error("[ERROR] ShardNum[%v] is to large", idxstruct.ShardNum)
		return fmt.Errorf("[ERROR] ShardNum[%v] is to large", idxstruct.ShardNum)
	}
	var nodeinfo utils.NodeIndex
	nodeinfo.IndexName = idxstruct.IndexName
	nodeinfo.ShardNum = idxstruct.ShardNum
	nodeinfo.IndexMapping = idxstruct.IndexMapping
	nodeinfo.ShardNodes = make(map[uint64][]string)
	//nodeinfo.Shard = make([]uint64, idxstruct.ShardNum)
	for i := uint64(0); i < idxstruct.ShardNum; i++ {
		si := ShardInfo{ShardPathName: fmt.Sprintf("%v/%v_%v", utils.IDX_DETAIL_PATH, idxstruct.IndexName, i),
			MaxOffset: 0, NodeInfos: make([]string, 0)}
		si.OffsetMmap, _ = utils.NewMmap(fmt.Sprintf("%v/%v_%v.offset", utils.IDX_DETAIL_PATH, idxstruct.IndexName, i), utils.MODE_APPEND)
		si.DetailMmap, _ = utils.NewMmap(fmt.Sprintf("%v/%v_%v.dtl", utils.IDX_DETAIL_PATH, idxstruct.IndexName, i), utils.MODE_APPEND)

		for {
			seed := rand.Intn(len(seeds))
			if seeds[seed] == false {
				seeds[seed] = true
				net := make([]string, 0)
				si.NodeInfos = append(si.NodeInfos, fmt.Sprintf("%v:%v", this.NodeInfos[seed].Addr, this.NodeInfos[seed].MPort))
				net = append(net, fmt.Sprintf("%v:%v", this.NodeInfos[seed].Addr, this.NodeInfos[seed].MPort))

				nodeinfo.ShardNodes[uint64(seed)] = net
				this.Logger.Info("[INFO] NodeInfos::: %v", si.NodeInfos)
				break
			}
		}

		sis = append(sis, si)

	}
	info := idxInfo{Name: idxstruct.IndexName, ShardNum: idxstruct.ShardNum, MaxCnt: 0,
		IndexMapping: idxstruct.IndexMapping, ShardInfos: sis, ShardField: idxstruct.ShardField}

	this.IndexInfo[idxstruct.IndexName] = info

	for _, iinfo := range this.NodeInfos {
		iinfo.IdxChan <- nodeinfo
	}

	return this.storeStruct()

}

func (this *Dispatcher) UpdateDocument(method string, parms map[string]string, body []byte) (string, error) {

	/*cid, hascid := parms["cid"]

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

func (this *Dispatcher) PullDetail(method string, parms map[string]string, body []byte) ([]string, uint64) {
	//indexname string, shardNum uint64, docid uint64, lens uint64
	indexname, hasidx := parms["index"]
	shardNumstr, hasshardnum := parms["shardnum"]
	docidstr, hasdocid := parms["start"]
	lensstr, haslens := parms["lens"]

	var err error
	var shardNum, docid, lens uint64

	if !hasidx || !hasshardnum || !hasdocid || !haslens {
		this.Logger.Error("[ERROR] parms error ")
		return nil, 0
	}

	shardNum, err = strconv.ParseUint(shardNumstr, 0, 0)
	if err != nil {
		this.Logger.Error("[ERROR] parms error %v ", shardNumstr)
		return nil, 0
	}
	docid, err = strconv.ParseUint(docidstr, 0, 0)
	if err != nil {
		this.Logger.Error("[ERROR] parms error %v ", docidstr)
		return nil, 0
	}
	lens, err = strconv.ParseUint(lensstr, 0, 0)
	if err != nil {
		this.Logger.Error("[ERROR] parms error %v ", lensstr)
		return nil, 0
	}

	res := make([]string, 0)
	enddocid := docid

	if _, ok := this.IndexInfo[indexname]; !ok {
		return nil, 0
	}

	if shardNum >= this.IndexInfo[indexname].ShardNum {
		return nil, 0
	}

	if docid >= this.IndexInfo[indexname].ShardInfos[shardNum].MaxOffset {
		return nil, 0
	}

	for enddocid = docid; ; enddocid++ {
		offst := this.IndexInfo[indexname].ShardInfos[shardNum].OffsetMmap.ReadUInt64(enddocid * 8)
		dtl := this.IndexInfo[indexname].ShardInfos[shardNum].DetailMmap.ReadStringWith32Bytes(int64(offst))
		res = append(res, dtl)
		if enddocid == docid+lens || enddocid == this.IndexInfo[indexname].ShardInfos[shardNum].MaxOffset {
			break
		}
	}
	return res, enddocid
}

func (this *Dispatcher) updateDocument(indexname string, content string) error {

	if _, ok := this.IndexInfo[indexname]; !ok {
		return fmt.Errorf("index[%v] not found", indexname)
	}

	if this.IndexInfo[indexname].ShardField != "" {
		document := make(map[string]string)
		if err := json.Unmarshal([]byte(content), &document); err != nil {
			this.Logger.Error("[ERROR]  %v \t %v ", err, content)
			return err
		}

	}

	shardnum := this.IndexInfo[indexname].ShardNum

	/*
		for _, field := range this.IndexInfo[indexname].IndexMapping {
			if _, ok := document[field.FieldName]; !ok {
				document[field.FieldName] = ""
			}
		}
	*/

	seed := rand.Intn(int(shardnum))
	this.IndexInfo[indexname].ShardInfos[seed].DetailMmap.AppendStringWithLen(content)
	offset := this.IndexInfo[indexname].ShardInfos[seed].DetailMmap.GetPointer()
	this.IndexInfo[indexname].ShardInfos[seed].OffsetMmap.AppendUInt64(uint64(offset))
	this.IndexInfo[indexname].ShardInfos[seed].MaxOffset++

	return nil

}

func (this *Dispatcher) LoadData(method string, parms map[string]string, body []byte) (string, error) {

	//cid, hascid := parms["cid"]

	indexname, hasindexname := parms["index"]

	if !hasindexname || method != "POST" {
		return eDefaultEngineLoadFail, errors.New(eProcessoParms)
	}

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
	document := make(map[string]string)
	flen := len(loadstruct.Fields)
	for scanner.Scan() {
		//content := make(map[string]string)
		var textcontent string
		if isJson {
			textcontent = scanner.Text()

			//if err := json.Unmarshal([]byte(textcontent), &content); err != nil {
			//this.Logger.Error("[INFO]  %v \t %v ", err, scanner.Text())
			//	continue
			//}
			contentlist := strings.Split(textcontent, "\t")
			if len(contentlist) != flen {
				continue
			}
			for i, field := range loadstruct.Fields {
				document[field] = contentlist[i]
			}
			btext, _ := json.Marshal(document)

			this.updateDocument(indexname, string(btext))

		} else {
			return "", errors.New(eProcessoParms)
		}
		rcount++
		if rcount%10000 == 0 {
			this.Logger.Info("[INFO] Read Record [ %v ]", rcount)
			this.sync()
		}

	}

	this.sync()

	return eDefaultEngineLoadOk, nil

}

func (this *Dispatcher) storeStruct() error {
	metaFileName := fmt.Sprintf("%v/Dispatcher.json", utils.IDX_DETAIL_PATH)
	if err := utils.WriteToJson(this, metaFileName); err != nil {
		this.Logger.Error("[ERROR] storeStruct %v", err)
		return err
	}
	return nil
}

func (this *Dispatcher) sync() error {

	for k, _ := range this.IndexInfo {
		for i, _ := range this.IndexInfo[k].ShardInfos {
			this.IndexInfo[k].ShardInfos[i].DetailMmap.Sync()
			this.IndexInfo[k].ShardInfos[i].OffsetMmap.Sync()
		}
	}

	return this.storeStruct()
}

func (this *Dispatcher) controlThread(netinfo *utils.NodeNetInfo) {

	for {

		select {
		case e1 := <-netinfo.IdxChan:
			this.Logger.Info("[INFO] indexNodeChan:: %v", e1)
			url := fmt.Sprintf("http://%v:%v/v1/_heart?type=addindex", netinfo.Addr, netinfo.MPort)
			post, e := json.Marshal(e1)
			if e != nil {
				this.Logger.Error("[ERROR] post json error %v", e)
			}
			res, err := utils.PostRequest(url, post)
			if err != nil {
				this.Logger.Error("[ERROR] error %v", err)
			} else {
				r := make(map[string]interface{})
				if err := json.Unmarshal(res, &r); err != nil {
					this.Logger.Error("[ERROR] json error %v", err)
				} else {
					this.Logger.Info("[INFO] Hearting Check ===> Addr:[%v:%v] Status : [%v]", netinfo.Addr, netinfo.MPort, r["_status"])
				}

			}

		case <-time.After(2 * time.Second):
			//this.Logger.Info("[INFO]  %v", time.Now())
			url := fmt.Sprintf("http://%v:%v/v1/_heart?type=alive", netinfo.Addr, netinfo.MPort)
			res, err := utils.RequestUrl(url)
			if err != nil {
				this.Logger.Error("[ERROR] error %v", err)
			} else {
				r := make(map[string]interface{})
				if err := json.Unmarshal(res, &r); err != nil {
					this.Logger.Error("[ERROR] json error %v", err)
				} else {
					this.Logger.Info("[INFO] Hearting Check ===> Addr:[%v:%v] Status : [%v]", netinfo.Addr, netinfo.MPort, r["_status"])
				}

			}

		}

	}

}

func (this *Dispatcher) Heart(method string, parms map[string]string, body []byte) (map[string]string, error) {

	return nil, nil
}

func (this *Dispatcher) InitEngine() error {

	return nil
}
