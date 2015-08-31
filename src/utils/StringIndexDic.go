/*****************************************************************************
 *  file name : StringIndexDic.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 字符串到ID的hash映射文件，可以为每个term生成唯一的ID，用于后续的
 *   				   倒排索引
 *
******************************************************************************/

package utils

import (
	"fmt"
)

type StringItemDic struct {
	HashCode int64
	Key      string
	Value    int64
	Next     int64
}

type StringIdxDic struct {
	Entity      []StringItemDic
	HashIndex   []int64
	EntityCount int64
	Bukets      int64
	Lens        int64
	StringMap	map[string]int64
	Index    	int64
	
}







func NewStringIdxDic(bukets int64) *StringIdxDic {
	/*
	this := &StringIdxDic{EntityCount: 1, Bukets: bukets}
	this.Lens = bukets
	this.Entity = make([]StringItemDic, this.Lens, 100000)
	this.HashIndex = make([]int64, this.Lens, 100000)
	*/
	this := &StringIdxDic{StringMap:make(map[string]int64),Lens:0,Index:1}
	return this
}

func (this *StringIdxDic) Display() {
	
	fmt.Printf("========================= Lens : %v  Count :%v =========================\n", this.Lens, this.Index)
	for k,v := range this.StringMap {
		fmt.Printf("Key : %v \t\t--- Value : %v  \n", k, v)
	}
	fmt.Printf("===============================================================================\n")
	
}

/*****************************************************************************
*  function name : PutKeyForInt
*  params : 输入的key
*  return :
*
*  description : 在hash表中添加一个key，产生一个key的唯一id
*
******************************************************************************/
func (this *StringIdxDic) Put(key string) int64 {
	
	id:= this.Find(key)
	if id!=-1{
		return id
	}
	
	this.StringMap[key] = this.Index
	this.Index ++ 
	this.Lens ++
	
	return this.StringMap[key]
	/*
	//桶已经满了，不能添加
	if this.EntityCount == this.Lens {
		fmt.Printf("[StringIdxDic] Bukets Full...Append arrays [EntityCount : %v] [Lens : %v] \n", this.EntityCount, this.Lens)
		e := make([]StringItemDic, this.Bukets)
		h := make([]int64, this.Bukets)
		this.Entity = append(this.Entity, e...)
		this.HashIndex = append(this.HashIndex, h...)
		this.Lens = this.Lens + this.Bukets
		fmt.Printf("[StringIdxDic] Bukets Full...Append arrays [ New EntityCount : %v] [ New Lens : %v] \n", this.EntityCount, this.Lens)
	}
	//已经添加过了，返回ID值
	id := this.Find(key)
	if id != -1 {
		return id
	}
	hash := ELFHash(key, this.Bukets)
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
func (this *StringIdxDic) Length() int64 {
	
	return this.Lens
//	return this.EntityCount - 1
}

func (this *StringIdxDic) Find(key string) int64 {
	
	value,has_key:=this.StringMap[key]
	if has_key{
		return value
	}
	return -1
	/*
	hash := ELFHash(key, this.Bukets)
	var k int64
	for k = this.HashIndex[hash]; k != 0; k = this.Entity[k].Next {
		if key == this.Entity[k].Key {
			//fmt.Printf("K :%v ==== Value : %v\n",k,this.Entity[k].ValueInt)
			return this.Entity[k].Value
		}
	}
	return -1
	*/
}
