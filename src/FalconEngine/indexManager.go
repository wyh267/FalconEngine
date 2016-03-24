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
)


type IndexInfo struct {
    Name    string `json:"name"`
    Path    string `json:"path"`
}



type IndexMgt struct {
    indexers   map[string]*fi.Index
    IndexInfos map[string]IndexInfo `json:"indexinfos"`
}



