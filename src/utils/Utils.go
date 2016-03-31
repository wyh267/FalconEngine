package utils

import (
	"container/list"
	"math"
	"os"
	"time"
)

// IDX_ROOT_PATH 默认索引存放位置
const IDX_ROOT_PATH string = "./index/"
const DEFAULT_PRIMARY_KEY string = "_id"
const FALCONSEARCHERNAME string = "FALCONENGINE"

// DocIdNode docid的基本结构体，包括docid和权重
type DocIdNode struct {
	Docid  uint32
	Weight uint32
	//Pos    uint32
}

//DocIdSort 按照docid进行排序
type DocIdSort []DocIdNode

func (a DocIdSort) Len() int      { return len(a) }
func (a DocIdSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a DocIdSort) Less(i, j int) bool {
	if a[i] == a[j] {
		return a[i].Docid < a[j].Docid
	}
	return a[i].Docid < a[j].Docid
}

//DocWeightSort 按照weight进行排序
type DocWeightSort []DocIdNode

func (a DocWeightSort) Len() int      { return len(a) }
func (a DocWeightSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a DocWeightSort) Less(i, j int) bool {
	if a[i] == a[j] {
		return a[i].Weight > a[j].Weight
	}
	return a[i].Weight > a[j].Weight
}

const DOCNODE_SIZE int = 8 //12

// BASE_PREFIX_SEGMENT 段起始计数
const BASE_PREFIX_SEGMENT uint64 = 1000

// 索引类型说明
const (
	IDX_TYPE_STRING      = 1 //字符型索引[全词匹配]
	IDX_TYPE_STRING_SEG  = 2 //字符型索引[切词匹配，全文索引,hash存储倒排]
	IDX_TYPE_STRING_LIST = 3 //字符型索引[列表类型，分号切词，直接切分,hash存储倒排]

	IDX_TYPE_NUMBER = 11 //数字型索引，只支持整数，数字型索引只建立正排

	IDX_TYPE_DATE = 15 //日期型索引 '2015-11-11 00:11:12'，日期型只建立正排，转成时间戳存储

	IDX_TYPE_PK = 21 //主键类型，倒排正排都需要，倒排使用B树存储
	GATHER_TYPE = 22 //汇总类型，倒排正排都需要[后续使用]

	IDX_ONLYSTORE = 30 //只保存详情，不参与检索
)

// 过滤类型，对应filtertype
const (
	FILT_EQ    uint64 = 1 //等于
	FILT_OVER  uint64 = 2 //大于
	FILT_LESS  uint64 = 3 //小于
	FILT_RANGE uint64 = 4 //范围内
)

// SimpleFieldInfo description: 字段的描述信息
type SimpleFieldInfo struct {
	FieldName string `json:"fieldname"`
	FieldType uint64 `json:"fieldtype"`
	PflOffset int64  `json:"pfloffset"` //正排索引的偏移量
	PflLen    int    `json:"pfllen"`    //正排索引长度
}

// IndexStrct 索引构造结构，包含字段信息
type IndexStrct struct {
	IndexName    string            `json:"indexname"`
	IndexMapping []SimpleFieldInfo `json:"indexmapping"`
}

//Term分词信息
type TermInfo struct {
	Term string
	Tf   int
}

/*************************************************************************
索引查询接口
索引查询分为 查询和过滤,统计，子查询四种
查询：倒排索引匹配
过滤：正排索引过滤
统计：汇总某个字段，然后进行统计计算
************************************************************************/
// FSSearchQuery function description : 查询接口数据结构[用于倒排索引查询]，内部都是求交集
type FSSearchQuery struct {
	FieldName string `json:"_field"`
	Value     string `json:"_value"`
	Type      uint64 `json:"_type"`
}

// FSSearchCrossFieldsQuery description : 跨字段搜索，满足一个就返回
type FSSearchCrossFieldsQuery struct {
	FieldNames []string `json:"_fields"`
	Value      string   `json:"_value"`
	Type       uint64   `json:"_type"`
}

// FSSearchFilted function description : 过滤接口数据结构，内部都是求交集
type FSSearchFilted struct {
	FieldName string `json:"_field"`
	Start     int64  `json:"_start"`
	End       int64  `json:"_end"`
	Type      uint64 `json:"_type"`
}

// FSSearchGather description : 汇总
type FSSearchGather struct {
	FieldNames []string `json:"_gatherfields"`
}

//统计类型
const (
	OP_COUNT uint64 = 1
	OP_SUM   uint64 = 2
	OP_AVG   uint64 = 3
	OP_MAX   uint64 = 4
	OP_MIN   uint64 = 5
)

// FSStatistics function description : 汇总统计接口数据结构
type FSStatistics struct {
	Gather string `json:"_gather"` //汇总字段
	Op     uint64 `json:"_op"`     //统计字段的操作
	Field  string `json:"_field"`  //统计字段
	Type   uint64 `json:"_type"`   //统计后操作的类型
	Start  int64  `json:"_start"`  //统计后操作的起始范围
	End    int64  `json:"_end"`    //统计后操作的结束范围
}

/*************************************************************************
查询返回的数据结构项
************************************************************************/
// FEStatisticsResultMap function description : 统计结果结构
type FSStatisticsResultMap struct {
	Gather string           `json:"_gather"`
	Info   map[string]int64 `json:"_info"`
}

// FEStatisticsResult function description : 统计结果结构
type FSStatisticsResult struct {
	ResultCount    uint64                  `json:"_resultcount"`
	StatisticsInfo []FSStatisticsResultMap `json:"_statistics"`
}

// FEResultNode function description : 返回结果的单节点信息
type FSResultNode struct {
	Info map[string]string `json:"_information"`
}

// FSResult function description : 返回总结构
type FSResult struct {
	Statistics FSStatisticsResult `json:"_stat"`   //统计信息
	ResultInfo []FSResultNode     `json:"_result"` //详情信息
}

// FEResultAutomatic struct description : 营销自动化返回接口
type FEResultAutomatic struct {
	Count    uint64   `json:"_count"`
	Contacts []string `json:"_contactids"`
}

// FEResultAutomatic struct description : 营销自动化返回接口
type FEResultAutomaticSingle struct {
	ContactID  string `json:"_contactid"`
	HasContact int    `json:"_hascontact"`
	Condition  int    `json:"_condition"`
}

type FSLoadStruct struct {
	Split     string   `json:"_split"`
	Fields    []string `json:"_fields"`
	Filename  string   `json:"_filename"`
	SyncCount int      `json:"_synccount"`
	IsMerge   bool     `json:"_ismerge"`
}

// Engine 引擎接口，自己实现的引擎必须实现以下接口
type Engine interface {
	Search(method string, parms map[string]string, body []byte) (string, error)
	CreateIndex(method string, parms map[string]string, body []byte) error
	UpdateDocument(method string, parms map[string]string, body []byte) (string, error)
	LoadData(method string, parms map[string]string, body []byte) (string, error)
}

/*****************************************************************************
*  function name : Merge
*  params :
*  return :
*
*  description : 求并集
*
******************************************************************************/
func Merge(a []DocIdNode, b []DocIdNode) ([]DocIdNode, bool) {
	lena := len(a)
	lenb := len(b)
	lenc := 0
	c := make([]DocIdNode, lena+lenb)
	ia := 0
	ib := 0
	//fmt.Printf("Lena : %v ======== Lenb : %v \n",lena,lenb)
	if lena == 0 && lenb == 0 {
		return nil, false
	}

	for ia < lena && ib < lenb {

		if a[ia] == b[ib] {
			//c = append(c, a[ia])
			c[lenc] = a[ia]
			lenc++
			ia++
			ib++
			continue
		}

		if a[ia].Docid < b[ib].Docid {
			//	fmt.Printf("ia : %v ======== ib : %v \n",ia,ib)
			//c = append(c, a[ia])
			c[lenc] = a[ia]
			lenc++
			ia++
		} else {
			//c = append(c, b[ib])
			c[lenc] = b[ib]
			lenc++
			ib++
		}
	}

	if ia < lena {
		for ; ia < lena; ia++ {
			//c = append(c, a[ia])
			c[lenc] = a[ia]
			lenc++
		}

	} else {
		for ; ib < lenb; ib++ {
			//c = append(c, b[ib])
			c[lenc] = b[ib]
			lenc++
		}
	}

	return c[:lenc], true

}

// ComputeWeight function description : 计算权重
// params :
// return :
func ComputeWeight(res []DocIdNode, df int, maxdoc uint32) []DocIdNode {
	idf := math.Log10(float64(maxdoc) / float64(df))
	for ia := 0; ia < len(res); ia++ {
		res[ia].Weight = uint32(float64(res[ia].Weight) * idf)
	}
	return res

}

// ComputeTfIdf function description : 计算TFIDF
// params :
// return :
func ComputeTfIdf(res []DocIdNode, a []DocIdNode, df int, maxdoc uint32) []DocIdNode {

	for ia := 0; ia < len(a); ia++ {
		weight := uint32((float64(a[ia].Weight) / 10000 * math.Log10(float64(maxdoc)/float64(df))) * 1000)
		docid := a[ia].Docid
		res = append(res, DocIdNode{Docid: docid, Weight: weight})
	}
	return res
}

// InteractionWithStartAndDf function description : 求交集的同时进行TFIDF的计算
// params :
// return :
func InteractionWithStartAndDf(a []DocIdNode, b []DocIdNode, start int, df int, maxdoc uint32) ([]DocIdNode, bool) {

	if a == nil || b == nil {
		return a, false
	}

	lena := len(a)
	lenb := len(b)
	lenc := start
	ia := start
	ib := 0
	idf := math.Log10(float64(maxdoc) / float64(df))
	for ia < lena && ib < lenb {

		if a[ia].Docid == b[ib].Docid {
			a[lenc] = a[ia]
			//uint32((float64(a[ia].Weight) / 10000 * idf ) * 10000)
			a[lenc].Weight += uint32(float64(a[ia].Weight) * idf)
			lenc++
			ia++
			ib++
			continue
			//c = append(c, a[ia])
		}

		if a[ia].Docid < b[ib].Docid {
			ia++
		} else {
			ib++
		}
	}

	return a[:lenc], true
}

func InteractionWithStart(a []DocIdNode, b []DocIdNode, start int) ([]DocIdNode, bool) {

	if a == nil || b == nil {
		return a, false
	}

	lena := len(a)
	lenb := len(b)
	lenc := start
	ia := start
	ib := 0

	//fmt.Printf("a:%v,b:%v,c:%v\n",lena,lenb,lenc)
	for ia < lena && ib < lenb {

		if a[ia].Docid == b[ib].Docid {
			a[lenc] = a[ia]
			lenc++
			ia++
			ib++
			continue
			//c = append(c, a[ia])
		}

		if a[ia].Docid < b[ib].Docid {
			ia++
		} else {
			ib++
		}
	}

	//fmt.Printf("a:%v,b:%v,c:%v\n",lena,lenb,lenc)
	return a[:lenc], true

}

/*****************************************************************************
*  function name : Interaction
*  params :
*  return :
*
*  description : 求交集
*
******************************************************************************/
func Interaction(a []DocIdNode, b []DocIdNode) ([]DocIdNode, bool) {

	if a == nil || b == nil {
		return nil, false
	}

	lena := len(a)
	lenb := len(b)
	var c []DocIdNode
	lenc := 0
	if lena < lenb {
		c = make([]DocIdNode, lena)
	} else {
		c = make([]DocIdNode, lenb)
	}
	//fmt.Printf("a:%v,b:%v,c:%v\n", lena, lenb, lenc)
	ia := 0
	ib := 0
	for ia < lena && ib < lenb {

		if a[ia].Docid == b[ib].Docid {
			c[lenc] = a[ia]
			lenc++
			ia++
			ib++
			continue
			//c = append(c, a[ia])
		}

		if a[ia].Docid < b[ib].Docid {
			ia++
		} else {
			ib++
		}
	}

	if len(c) == 0 {
		return nil, false
	} else {
		return c[:lenc], true
	}

}

// BinSearch function description : 二分查找
// params :
// return :
func BinSearch(docids []DocIdNode, item DocIdNode) int {

	low := 0
	high := len(docids) - 1
	if low > high {
		return -1
	}

	mid := (low + high) / 2
	midValue := docids[mid]
	if docids[mid].Docid > item.Docid {
		return BinSearch(docids[low:mid], item)
	}

	if docids[mid].Docid < item.Docid {
		return BinSearch(docids[mid+1:high+1], item)
	}

	if midValue == item {
		return mid
	}
	return -1

}

// Exist function description : 判断文件是否存在
// params :
// return :
func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

/*****************************
内存池使用
******************************/
type queued struct {
	when  time.Time
	slice []DocIdNode
}

type docqueued struct {
	when  time.Time
	slice []DocIdNode
}

const MAX_DOCID_LEN = 1000

func makeDocIdSlice() []DocIdNode {
	return make([]DocIdNode, 0, MAX_DOCID_LEN)
}

var GetDocIDsChan chan []DocIdNode
var GiveDocIDsChan chan []DocIdNode

func DocIdsMaker() (get, give chan []DocIdNode) {
	get = make(chan []DocIdNode)
	give = make(chan []DocIdNode)
	go func() {
		q := new(list.List)

		for {
			if q.Len() == 0 {
				q.PushFront(queued{when: time.Now(), slice: makeDocIdSlice()})
			}

			e := q.Front()

			timeout := time.NewTimer(time.Minute)
			select {
			case b := <-give:
				timeout.Stop()
				//fmt.Printf("Recive Buffer...\n")
				//b=b[:MAX_DOCID_LEN]
				q.PushFront(queued{when: time.Now(), slice: b})

			case get <- e.Value.(queued).slice[:0]:
				timeout.Stop()
				//fmt.Printf("Sent Buffer...\n")
				q.Remove(e)

			case <-timeout.C:
				e := q.Front()
				for e != nil {
					n := e.Next()
					if time.Since(e.Value.(queued).when) > time.Minute {
						q.Remove(e)
						e.Value = nil
					}
					e = n
				}

			}
		}

	}()

	return
}

// IsDateTime function description : 判断是否是日期时间格式
// params : 字符串
// return : 是否是日期时间格式
func IsDateTime(datetime string) (int64, error) {

	var timestamp time.Time
	var err error

	if len(datetime) > 10 {
		timestamp, err = time.ParseInLocation("2006-01-02 15:04:05", datetime, time.Local)
		if err != nil {
			return 0, err
		}
	} else {
		timestamp, err = time.ParseInLocation("2006-01-02", datetime, time.Local)
		if err != nil {
			return 0, err
		}
	}

	return timestamp.Unix(), nil

}

// FormatDateTime function description : 将时间戳变化成日期时间格式
// params :
// return :
func FormatDateTime(timestamp int64) (string, bool) {

	if timestamp == 0 {
		return "", false
	}
	tm := time.Unix(timestamp, 0)
	return tm.Format("2006-01-02 15:04:05"), true

}
