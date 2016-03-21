package utils

import "os"

// IDX_ROOT_PATH 默认索引存放位置
const IDX_ROOT_PATH string = "./index/"

// FALCONENGINENAME base名称
const FALCONSEARCHERNAME string = "FALCONENGINE"

type DocIdNode uint32

type DocIdSort []DocIdNode

func (a DocIdSort) Len() int      { return len(a) }
func (a DocIdSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a DocIdSort) Less(i, j int) bool {
	if a[i] == a[j] {
		return a[i] < a[j]
	}
	return a[i] < a[j]
}

const DOCNODE_SIZE int = 4

const BASE_PREFIX_SEGMENT uint64 = 1000

const SIZE_OF_TRIE_NODE int = 10

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
	PflOffset int64          `json:"pfloffset"` //正排索引的偏移量
	PflLen    int            `json:"pfllen"`    //正排索引长度
}



/*************************************************************************
索引查询接口
索引查询分为 查询和过滤,统计，子查询四种
查询：倒排索引匹配
过滤：正排索引过滤
统计：汇总某个字段，然后进行统计计算
子查询：必须是有父子
************************************************************************/
// FSSearchQuery function description : 查询接口数据结构[用于倒排索引查询]，内部都是求交集
type FSSearchQuery struct {
	FieldName string `json:"_field"`
	Value     string `json:"_value"`
	Type      uint64 `json:"_type"`
}

// FSSearchFilted function description : 过滤接口数据结构，内部都是求交集
type FSSearchFilted struct {
	FieldName string `json:"_field"`
	Start     int64  `json:"_start"`
	End       int64  `json:"_end"`
	Type      uint64 `json:"_type"`
}

type FSSearchUnit struct {
    IndexName  string           `json:"indexname"`
	Querys     []FSSearchQuery  `json:"_querys"`
	Filters    []FSSearchFilted `json:"_filters"`
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
	Gather   string `json:"_gather"`    //汇总字段
	Op       uint64 `json:"_op"`        //统计字段的操作
	Field    string `json:"_field"`     //统计字段
	Type     uint64 `json:"_type"`      //统计后操作的类型
	Start    int64  `json:"_start"`     //统计后操作的起始范围
	End      int64  `json:"_end"`       //统计后操作的结束范围
	StartStr string `json:"_start_str"` //统计后操作的起始范围
	EndStr   string `json:"_end_str"`   //统计后操作的结束范围
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

		if a[ia] < b[ib] {
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

		if a[ia] == b[ib] {
			a[lenc] = a[ia]
			lenc++
			ia++
			ib++
			continue
			//c = append(c, a[ia])
		}

		if a[ia] < b[ib] {
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

		if a[ia] == b[ib] {
			c[lenc] = a[ia]
			lenc++
			ia++
			ib++
			continue
			//c = append(c, a[ia])
		}

		if a[ia] < b[ib] {
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

func BinSearch(docids []DocIdNode, item DocIdNode) int {

	low := 0
	high := len(docids) - 1
	if low > high {
		return -1
	}

	mid := (low + high) / 2
	midValue := docids[mid]
	if docids[mid] > item {
		return BinSearch(docids[low:mid], item)
	}

	if docids[mid] < item {
		return BinSearch(docids[mid+1:high+1], item)
	}

	if midValue == item {
		return mid
	}
	return -1

}

func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}
