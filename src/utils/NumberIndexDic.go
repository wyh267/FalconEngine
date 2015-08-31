/*****************************************************************************
 *  file name : NumberIndexDic.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数字到ID的hash映射文件，可以为每个数字生成唯一的ID，用于后续的
 *   				   倒排索引
 *
******************************************************************************/

package utils

import (
	"fmt"
)

type NumItemDic struct {
	HashCode int64
	Key      int64
	Value    int64
	Next     int64
}

type NumberIdxDic struct {
	/*
	Entity      []NumItemDic
	HashIndex   []int64
	EntityCount int64
	Bukets      int64
	*/
	Lens        int64
	IntMap		map[string]int64
	Index    	int64
}

/*****************************************************************************
*  function name : NewNumberIdxDic
*  params : buket_type ===> 1: 2: 3:
*  return : NumberIdxDic
*
*  description :
*
******************************************************************************/
func NewNumberIdxDic(buket_type int64) *NumberIdxDic {
	/*
	var bukets int64
	switch buket_type {
	case 1:
		bukets = 5001
	case 2:
		bukets = 50001
	case 3:
		bukets = 50001
	case 4:
		bukets = 50001
	default:
		bukets = 5001

	}
	this := &NumberIdxDic{EntityCount: 1, Bukets: bukets}
	this.Lens = bukets
	this.Entity = make([]NumItemDic, this.Lens, 1000000)
	this.HashIndex = make([]int64, this.Lens, 1000000)
	*/
	this := &NumberIdxDic{IntMap:make(map[string]int64),Lens:0,Index:1}
	return this
}

func (this *NumberIdxDic) Display() {
	fmt.Printf("========================= Bukets : %v  EntityCount :%v =========================\n", this.Lens, this.Index)

	for k,v := range this.IntMap {
		fmt.Printf("Key : %v \t\t--- Value : %v  \n", k,v)
	}
	fmt.Printf("===============================================================================\n")
}

func (this *NumberIdxDic) Put(key int64) int64 {
	
	key_str:=fmt.Sprintf("%v",key)
	id,_:= this.Find(key)
	if id!=-1{
		return id
	}
	
	this.IntMap[key_str] = this.Index
	this.Index ++ 
	this.Lens ++
	//fmt.Printf("Add Key %v ,value is : %v \n",key,this.IntMap[key_str])
	return this.IntMap[key_str]
	/*
	//桶已经满了，不能添加
	if this.EntityCount == this.Lens {
		//fmt.Printf("Bukets Full...Append arrays [EntityCount : %v] [Lens : %v] \n",this.EntityCount,this.Lens)
		e := make([]NumItemDic, this.Bukets)
		h := make([]int64, this.Bukets)
		this.Entity = append(this.Entity, e...)
		this.HashIndex = append(this.HashIndex, h...)
		this.Lens = this.Lens + this.Bukets
		//fmt.Printf("Bukets Full...Append arrays [ New EntityCount : %v] [ New Lens : %v] \n",this.EntityCount,this.Lens)
	}
	//已经添加过了，返回ID值
	id, hash := this.Find(key)
	if id != -1 {
		return id
	}
	//hash:= ModHash(key,this.Bukets)
	this.Entity[this.EntityCount].HashCode = hash
	this.Entity[this.EntityCount].Key = key
	this.Entity[this.EntityCount].Value = this.EntityCount
	this.Entity[this.EntityCount].Next = this.HashIndex[hash]
	this.HashIndex[hash] = this.EntityCount
	this.EntityCount++

	return this.EntityCount - 1
	*/

}

/*****************************************************************************
*  function name : Length
*  params : nil
*  return : int64
*
*  description : 返回哈希表长度
*
******************************************************************************/
func (this *NumberIdxDic) Length() int64 {
	
	return this.Lens
	//return this.EntityCount - 1
}

func (this *NumberIdxDic) Find(key int64) (int64, int64) {
	
	key_str:=fmt.Sprintf("%v",key)
	value,has_key:=this.IntMap[key_str]
	if has_key{
		return value,0
	}
	return -1,0
	
	/*
	hash := ModHash(key, this.Bukets)
	var k int64
	for k = this.HashIndex[hash]; k != 0; k = this.Entity[k].Next {
		if key == this.Entity[k].Key {
			//fmt.Printf("K :%v ==== Value : %v\n",k,this.Entity[k].ValueInt)
			return this.Entity[k].Value, hash
		}
	}
	return -1, hash
	*/
}

/*****************************************************************************
*  function name : ModHash
*  params : int64
*  return : int64
*
*  description : 整数hash函数
*
******************************************************************************/
func ModHash(key, bukets int64) int64 {

	return key % bukets
}
