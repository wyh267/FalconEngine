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
}

func NewStringIdxDic(bukets int64) *StringIdxDic {
	this := &StringIdxDic{EntityCount: 1, Bukets: bukets}
	this.Entity = make([]StringItemDic, bukets)
	this.HashIndex = make([]int64, bukets)
	return this
}

func (this *StringIdxDic) Display() {
	fmt.Printf("========================= Bukets : %v  EntityCount :%v =========================\n", this.Bukets, this.EntityCount-1)
	var i int64
	for i = 1; i < this.EntityCount; i++ {
		fmt.Printf("Key : %v \t\t--- Value : %v \t\t--- HashCode : %v \n", this.Entity[i].Key, this.Entity[i].Value, this.Entity[i].HashCode)
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
	//桶已经满了，不能添加
	if this.EntityCount == this.Bukets {
		//fmt.Printf("Full %v\n",this.EntityCount)
		return -1
	}
	//已经添加过了，返回ID值
	id := this.Find(key)
	if id != -1 {
		//fmt.Printf("Find Key %v --- > %v\n",key,id)
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
	return this.EntityCount - 1
}

func (this *StringIdxDic) Find(key string) int64 {
	hash := ELFHash(key, this.Bukets)
	var k int64
	for k = this.HashIndex[hash]; k != 0; k = this.Entity[k].Next {
		if key == this.Entity[k].Key {
			//fmt.Printf("K :%v ==== Value : %v\n",k,this.Entity[k].ValueInt)
			return this.Entity[k].Value
		}
	}
	return -1
}
