/*****************************************************************************
 *  file name : indexManager.go
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
	"fmt"
	"os"
	"utils"
)

const (
	IKeyword  string = "keyword"
	ICreative string = "creative"
	IAdgroup  string = "adgroup"
	ICampaign string = "campaign"
	IAccount  string = "account"
)

type SemSearchMgt struct {
	Cid string `json:"cid"`

	accountIdx  *fi.Index
	campaignIdx *fi.Index
	adgroupIdx  *fi.Index
	keywordsIdx *fi.Index
	meaningsIdx *fi.Index
	indexers    map[string]*fi.Index
	IndexInfos  map[string][]utils.SimpleFieldInfo `json:"indexinfos"`
	Logger      *utils.Log4FE                      `json"-"`
}

/*
type SimpleFieldInfo struct {
	FieldName string `json:"fieldname"`
	FieldType uint64 `json:"fieldtype"`
	PflOffset int64  `json:"pfloffset"` //正排索引的偏移量
	PflLen    int    `json:"pfllen"`    //正排索引长度
}

*/

func newSemSearchMgt(cid string, logger *utils.Log4FE) *SemSearchMgt {

	this := &SemSearchMgt{Cid: cid, keywordsIdx: nil, meaningsIdx: nil, adgroupIdx: nil,
		IndexInfos: make(map[string][]utils.SimpleFieldInfo), indexers: make(map[string]*fi.Index),
		Logger: logger}

	this.IndexInfos[IAccount] = make([]utils.SimpleFieldInfo, 0)
	this.IndexInfos[IAccount] = append(this.IndexInfos[IAccount], utils.SimpleFieldInfo{FieldName: "account_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[IAccount] = append(this.IndexInfos[IAccount], utils.SimpleFieldInfo{FieldName: "media_username", FieldType: utils.IDX_TYPE_STRING_SINGLE})
	this.IndexInfos[IAccount] = append(this.IndexInfos[IAccount], utils.SimpleFieldInfo{FieldName: "budget_type", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[IAccount] = append(this.IndexInfos[IAccount], utils.SimpleFieldInfo{FieldName: "budget", FieldType: utils.IDX_TYPE_NUMBER})

	this.IndexInfos[ICampaign] = make([]utils.SimpleFieldInfo, 0)
	this.IndexInfos[ICampaign] = append(this.IndexInfos[ICampaign], utils.SimpleFieldInfo{FieldName: "account_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[ICampaign] = append(this.IndexInfos[ICampaign], utils.SimpleFieldInfo{FieldName: "media_campaign_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[ICampaign] = append(this.IndexInfos[ICampaign], utils.SimpleFieldInfo{FieldName: "media_campaign_name", FieldType: utils.IDX_TYPE_STRING_SINGLE})
	this.IndexInfos[ICampaign] = append(this.IndexInfos[ICampaign], utils.SimpleFieldInfo{FieldName: "media_campaign_budget", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[ICampaign] = append(this.IndexInfos[ICampaign], utils.SimpleFieldInfo{FieldName: "media_campaign_status", FieldType: utils.IDX_TYPE_NUMBER})

	this.IndexInfos[IAdgroup] = make([]utils.SimpleFieldInfo, 0)
	this.IndexInfos[IAdgroup] = append(this.IndexInfos[IAdgroup], utils.SimpleFieldInfo{FieldName: "account_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[IAdgroup] = append(this.IndexInfos[IAdgroup], utils.SimpleFieldInfo{FieldName: "media_campaign_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[IAdgroup] = append(this.IndexInfos[IAdgroup], utils.SimpleFieldInfo{FieldName: "media_adgroup_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[IAdgroup] = append(this.IndexInfos[IAdgroup], utils.SimpleFieldInfo{FieldName: "media_adgroup_name", FieldType: utils.IDX_TYPE_STRING_SINGLE})
	this.IndexInfos[IAdgroup] = append(this.IndexInfos[IAdgroup], utils.SimpleFieldInfo{FieldName: "media_adgroup_status", FieldType: utils.IDX_TYPE_NUMBER})

	this.IndexInfos[IKeyword] = make([]utils.SimpleFieldInfo, 0)
	this.IndexInfos[IKeyword] = append(this.IndexInfos[IKeyword], utils.SimpleFieldInfo{FieldName: "_pk", FieldType: utils.IDX_TYPE_PK})
	this.IndexInfos[IKeyword] = append(this.IndexInfos[IKeyword], utils.SimpleFieldInfo{FieldName: "account_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[IKeyword] = append(this.IndexInfos[IKeyword], utils.SimpleFieldInfo{FieldName: "media_campaign_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[IKeyword] = append(this.IndexInfos[IKeyword], utils.SimpleFieldInfo{FieldName: "media_adgroup_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[IKeyword] = append(this.IndexInfos[IKeyword], utils.SimpleFieldInfo{FieldName: "media_keyword_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[IKeyword] = append(this.IndexInfos[IKeyword], utils.SimpleFieldInfo{FieldName: "media_keyword", FieldType: utils.IDX_TYPE_STRING_SINGLE})
	this.IndexInfos[IKeyword] = append(this.IndexInfos[IKeyword], utils.SimpleFieldInfo{FieldName: "media_keyword_status", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[IKeyword] = append(this.IndexInfos[IKeyword], utils.SimpleFieldInfo{FieldName: "match_type", FieldType: utils.IDX_TYPE_NUMBER})

	this.IndexInfos[ICreative] = make([]utils.SimpleFieldInfo, 0)
	this.IndexInfos[ICreative] = append(this.IndexInfos[ICreative], utils.SimpleFieldInfo{FieldName: "_pk", FieldType: utils.IDX_TYPE_PK})
	this.IndexInfos[ICreative] = append(this.IndexInfos[ICreative], utils.SimpleFieldInfo{FieldName: "account_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[ICreative] = append(this.IndexInfos[ICreative], utils.SimpleFieldInfo{FieldName: "media_campaign_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[ICreative] = append(this.IndexInfos[ICreative], utils.SimpleFieldInfo{FieldName: "media_adgroup_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[ICreative] = append(this.IndexInfos[ICreative], utils.SimpleFieldInfo{FieldName: "media_creative_id", FieldType: utils.IDX_TYPE_NUMBER})
	this.IndexInfos[ICreative] = append(this.IndexInfos[ICreative], utils.SimpleFieldInfo{FieldName: "media_creative_title", FieldType: utils.IDX_TYPE_STRING_SINGLE})
	this.IndexInfos[ICreative] = append(this.IndexInfos[ICreative], utils.SimpleFieldInfo{FieldName: "media_creative_description1", FieldType: utils.IDX_TYPE_STRING_SINGLE})
	this.IndexInfos[ICreative] = append(this.IndexInfos[ICreative], utils.SimpleFieldInfo{FieldName: "media_creative_description2", FieldType: utils.IDX_TYPE_STRING_SINGLE})
	this.IndexInfos[ICreative] = append(this.IndexInfos[ICreative], utils.SimpleFieldInfo{FieldName: "media_creative_status", FieldType: utils.IDX_TYPE_NUMBER})

	if utils.Exist(fmt.Sprintf("%v/%v/metainfo.json", utils.IDX_ROOT_PATH, this.Cid)) {
		metaFileName := fmt.Sprintf("%v/%v/metainfo.json", utils.IDX_ROOT_PATH, this.Cid)
		buffer, err := utils.ReadFromJson(metaFileName)
		if err != nil {
			return nil
		}

		err = json.Unmarshal(buffer, &this)
		if err != nil {
			return nil
		}

		for k := range this.IndexInfos {
			this.indexers[k] = fi.NewIndexWithLocalFile(k, fmt.Sprintf("%v/%v/", utils.IDX_ROOT_PATH, this.Cid), logger)
			if this.indexers[k] == nil {
				this.Logger.Error("[INFO]  New Index Manager error ")

				return nil
			}
		}

	} else {
		//os.
		os.MkdirAll(fmt.Sprintf("%v/%v/", utils.IDX_ROOT_PATH, this.Cid), os.ModeDir|os.ModePerm)

		for k, v := range this.IndexInfos {
			err := this.CreateIndex(k, fmt.Sprintf("%v/%v/", utils.IDX_ROOT_PATH, this.Cid), v)
			if err != nil {
				this.Logger.Error("[INFO]  Create New Index Manager error [%v]", err)

				return nil
			}

		}

	}

	return this

}

func (this *SemSearchMgt) CreateEmptyIndex(indexname string) error {

	if _, ok := this.indexers[indexname]; ok {
		this.Logger.Error("[ERROR] index[%v] Exist", indexname)
		return nil
	}

	this.indexers[indexname] = fi.NewEmptyIndex(indexname, utils.IDX_ROOT_PATH, this.Logger)
	return this.storeStruct()

}

func (this *SemSearchMgt) CreateIndex(indexname, pathname string, fields []utils.SimpleFieldInfo) error {

	if _, ok := this.indexers[indexname]; ok {
		this.Logger.Error("[ERROR] index[%v] Exist", indexname)
		return nil
	}

	this.indexers[indexname] = fi.NewEmptyIndex(indexname, pathname, this.Logger)
	for _, field := range fields {
		this.Logger.Info("[INFO] field %v", field)
		this.indexers[indexname].AddField(field)
	}

	return this.storeStruct()
}

func (this *SemSearchMgt) AddField(indexname string, field utils.SimpleFieldInfo) error {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname].AddField(field)
}

func (this *SemSearchMgt) storeStruct() error {
	metaFileName := fmt.Sprintf("%v/%v/metainfo.json", utils.IDX_ROOT_PATH, this.Cid)
	if err := utils.WriteToJson(this, metaFileName); err != nil {
		this.Logger.Error("[ERROR] storeStruct %v", err)
		return err
	}
	return nil
}

func (this *SemSearchMgt) updateDocument(indexname string, document map[string]string) (string, error) {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return "", fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return "{ \"status\":\"OK\" }", this.indexers[indexname].UpdateDocument(document)
}

func (this *SemSearchMgt) sync(indexname string) error {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname].SyncMemorySegment()
}

func (this *SemSearchMgt) mergeIndex(indexname string) error {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname].MergeSegments()
}

func (this *SemSearchMgt) Search(indexname string, querys []utils.FSSearchQuery, filters []utils.FSSearchFilted, ps, pg int) ([]map[string]string, bool) {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return nil, false //fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname].SimpleSearch(querys, filters, ps, pg)
}

func (this *SemSearchMgt) searchDocIds(indexname string,
	querys []utils.FSSearchQuery,
	filters []utils.FSSearchFilted) ([]utils.DocIdNode, bool) {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return nil, false //fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname].SearchDocIds(querys, filters)

}

func (this *SemSearchMgt) GetIndex(indexname string) *fi.Index {
	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return nil //fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname]
}

func (this *SemSearchMgt) syncAll() error {

	for k := range this.indexers {

		if err := this.sync(k); err != nil {
			return err
		}
	}

	return nil
}

func (this *SemSearchMgt) mergeAll() error {

	for k := range this.indexers {

		if err := this.mergeIndex(k); err != nil {
			return err
		}
	}

	return nil

}
