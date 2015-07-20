/*****************************************************************************
 *  file name : Index.gp
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 倒排索引基类
 *
******************************************************************************/

package indexer

import(
	u "utils"
)



type Index struct{
	Name		string				`json:"name"`
	Type		int64
	ivtIndex	*u.InvertIdx	`json:"ivtIndex"`
}


type IndexInterface interface {
	Find(term interface{}) ([]u.DocIdInfo,bool)
	Display()
	GetType() int64
}