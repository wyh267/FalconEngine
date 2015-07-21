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
	Entity      []NumItemDic
	HashIndex   []int64
	EntityCount int64
	Bukets      int64
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
	this := &NumberIdxDic{EntityCount: 1, Bukets: 701}
	this.Entity = make([]NumItemDic, 701)
	this.HashIndex = make([]int64, 701)
	return this
}

func (this *NumberIdxDic) Display() {
	fmt.Printf("========================= Bukets : %v  EntityCount :%v =========================\n", this.Bukets, this.EntityCount-1)
	var i int64
	for i = 1; i < this.EntityCount; i++ {
		fmt.Printf("Key : %v \t\t--- Value : %v  \t\t  --- HashCode : %v \n", this.Entity[i].Key, this.Entity[i].Value, this.Entity[i].HashCode)
	}
	fmt.Printf("===============================================================================\n")
}

func (this *NumberIdxDic) Put(key int64) int64 {
	//桶已经满了，不能添加
	if this.EntityCount == this.Bukets {
		return -1
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
	return this.EntityCount - 1
}

func (this *NumberIdxDic) Find(key int64) (int64, int64) {
	hash := ModHash(key, this.Bukets)
	var k int64
	for k = this.HashIndex[hash]; k != 0; k = this.Entity[k].Next {
		if key == this.Entity[k].Key {
			//fmt.Printf("K :%v ==== Value : %v\n",k,this.Entity[k].ValueInt)
			return this.Entity[k].Value, hash
		}
	}
	return -1, hash
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
