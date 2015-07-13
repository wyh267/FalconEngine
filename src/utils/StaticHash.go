
/*****************************************************************************
*  file name : StaticHash.go
*  author : Wu Yinghao
*  email  : wyh817@gmail.com
*
*  file description : 静态哈希文件实现
*
******************************************************************************/
package utils

import (
	"fmt"
)


type HashEntity struct{
	Hash_Code	int64
	Key			string
	Value		interface{}
	ValueInt	int64
	Next		int64
}


type StaticHashTable struct{
	Entity		[]HashEntity
	HashIndex	[]int64
	EntityCount	int64
	Bukets		int64
}


func NewStaticHashTable(bukets int64) *StaticHashTable{
	this := &StaticHashTable{EntityCount:1,Bukets:bukets}
	this.Entity = make([]HashEntity,bukets)
	this.HashIndex = make([]int64,bukets)
	return this	
}

/*****************************************************************************
*  function name : PutKeyForInt
*  params : 输入的key	
*  return : 
*
*  description : 在hash表中添加一个key，产生一个key的唯一id
*
******************************************************************************/
func (this *StaticHashTable) PutKeyForInt(key string) (int64){
		hash:= ELFHash(key,this.Bukets)
		if this.HashIndex[hash] == 0 {
			this.Entity[this.EntityCount].Hash_Code=hash
			this.Entity[this.EntityCount].Key=key
			this.Entity[this.EntityCount].ValueInt=this.EntityCount
			this.Entity[this.EntityCount].Next=this.HashIndex[hash]
			this.HashIndex[hash]=this.EntityCount
			this.EntityCount++
		}else{
			fmt.Printf("%v\n",this.Entity[this.HashIndex[hash]])
		}
		
		return this.EntityCount-1
        //entity[i].next = hashIndex[hash];// 典型的头插入法，由于hashIndex[]是全局变量，默认值是0，初次使用时0  
        //hashIndex[hash] = i;// 也就是说同一个hash值，第一次的hashIndex[]值为0；之后的上一次同一个hash值的每一次都是entity[]的下标值  
        //i++;
}



func (this *StaticHashTable) FindKey(key string) (int64){
	hash := ELFHash(key,this.Bukets)
	var k int64
	for k=this.HashIndex[hash]; k!=0; k=this.Entity[k].Next {  
        if key == this.Entity[k].Key {  
			fmt.Printf("K :%v ==== Value : %v\n",k,this.Entity[k].ValueInt)
            return this.Entity[k].ValueInt 
        }  
    }  
	return -1
}


func ELFHash(str string,bukets int64) int64 {  
    var hash int64
	var x int64
    for _,v:= range str{
		
		hash = (hash << 4) + int64(v)
		x=hash
		if (x & 0xF0000000) != 0{  
            hash ^= (x >> 24)  
            hash &= ^x
        }
	}   
    return (hash & 0x7FFFFFFF)%bukets
}








