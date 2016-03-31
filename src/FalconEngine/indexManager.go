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
	"utils"
)

type IndexInfo struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type IndexMgt struct {
	indexers   map[string]*fi.Index
	IndexInfos map[string]IndexInfo `json:"indexinfos"`
	Logger     *utils.Log4FE        `json"-"`
}

func newIndexMgt(logger *utils.Log4FE) *IndexMgt {

	this := &IndexMgt{indexers: make(map[string]*fi.Index),
		IndexInfos: make(map[string]IndexInfo),
		Logger:     logger}

	if utils.Exist(fmt.Sprintf("%v%v.mgt.meta", utils.IDX_ROOT_PATH, utils.FALCONSEARCHERNAME)) {

		metaFileName := fmt.Sprintf("%v%v.mgt.meta", utils.IDX_ROOT_PATH, utils.FALCONSEARCHERNAME)
		buffer, err := utils.ReadFromJson(metaFileName)
		if err != nil {
			return this
		}

		err = json.Unmarshal(buffer, &this)
		if err != nil {
			return this
		}

		for _, idxinfo := range this.IndexInfos {
			this.indexers[idxinfo.Name] = fi.NewIndexWithLocalFile(idxinfo.Name, idxinfo.Path, logger)
		}

	}
	this.Logger.Info("[INFO]  New Index Manager ")
	return this

}

func (this *IndexMgt) CreateEmptyIndex(indexname string) error {

	if _, ok := this.indexers[indexname]; ok {
		this.Logger.Error("[ERROR] index[%v] Exist", indexname)
		return nil
	}

	this.indexers[indexname] = fi.NewEmptyIndex(indexname, utils.IDX_ROOT_PATH, this.Logger)
	this.IndexInfos[indexname] = IndexInfo{Name: indexname, Path: utils.IDX_ROOT_PATH}
	return this.storeStruct()

}

func (this *IndexMgt) CreateIndex(indexname string, fields []utils.SimpleFieldInfo) error {

	if _, ok := this.indexers[indexname]; ok {
		this.Logger.Error("[ERROR] index[%v] Exist", indexname)
		return nil
	}

	this.indexers[indexname] = fi.NewEmptyIndex(indexname, utils.IDX_ROOT_PATH, this.Logger)
	this.IndexInfos[indexname] = IndexInfo{Name: indexname, Path: utils.IDX_ROOT_PATH}
	for _, field := range fields {
		this.indexers[indexname].AddField(field)
	}

	return this.storeStruct()
}

func (this *IndexMgt) AddField(indexname string, field utils.SimpleFieldInfo) error {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname].AddField(field)
}

func (this *IndexMgt) storeStruct() error {
	metaFileName := fmt.Sprintf("%v%v.mgt.meta", utils.IDX_ROOT_PATH, utils.FALCONSEARCHERNAME)
	if err := utils.WriteToJson(this, metaFileName); err != nil {
		return err
	}
	return nil
}

func (this *IndexMgt) updateDocument(indexname string, document map[string]string) (string, error) {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return "", fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return "{ \"status\":\"OK\" }", this.indexers[indexname].UpdateDocument(document)
}

func (this *IndexMgt) sync(indexname string) error {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname].SyncMemorySegment()
}

func (this *IndexMgt) mergeIndex(indexname string) error {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname].MergeSegments()
}


func (this *IndexMgt) searchDocIds(indexname string,
	querys []utils.FSSearchQuery,
	filters []utils.FSSearchFilted) ([]utils.DocIdNode, bool) {

	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return nil, false //fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname].SearchDocIds(querys, filters)

}

func (this *IndexMgt) GetIndex(indexname string) *fi.Index {
	if _, ok := this.indexers[indexname]; !ok {
		this.Logger.Error("[ERROR] index[%v] not found", indexname)
		return nil //fmt.Errorf("[ERROR] index[%v] not found", indexname)
	}

	return this.indexers[indexname]
}
