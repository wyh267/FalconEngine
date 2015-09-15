/*****************************************************************************
 *  file name : Index.gp
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 倒排索引基类，包括倒排的接口
 *
******************************************************************************/

package indexer

import (
	u "utils"
)

type Index struct {
	Name     string       `json:"name"`
	Type     int64        //1表示文本索引，2表示数字索引
	ivtIndex *u.InvertIdx `json:"ivtIndex"`
	customeInter u.CustomInterface
}

type IndexInterface interface {
	Find(term interface{}) ([]u.DocIdInfo, bool)
	Display()
	GetType() int64
	GetIvtIndex() *u.InvertIdx
	GetNumDic() *u.NumberIdxDic
	GetStrDic() *u.StringIdxDic
	SetCustomInterface(inter u.CustomInterface) error
	GetCustomInterface() u.CustomInterface
}
