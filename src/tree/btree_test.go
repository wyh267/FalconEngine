package tree

import (
	"fmt"
	"math/rand"
	"time"
	//"math/rand"
	"testing"
	"unsafe"
)

func TestNewTree(t *testing.T) {

	fmt.Printf("element len:%v\n", int(unsafe.Sizeof(element{})))
	db := NewBTDB("bp.tree")
	ok, value := db.Search("test", "hello")
	fmt.Printf("found hello : %v value : %v \n===================\n", ok, value)
	for i := 0; i < 30; i++ {
		db.AddBTree(fmt.Sprintf("%v", i))
	}
	db.AddBTree("test")
	//db.Set("test", "hello", 345)
	ok, value = db.Search("test", "hello")
	fmt.Printf("found hello : %v value : %v \n===================\n", ok, value)

	/*
	   	bt := NewEmptyBTree("bp.tree")
	   	//bt.display()

	   	//bt.put("hello",33)
	   	ok, value := bt.Search("hello")
	   	fmt.Printf("found hello : %v value : %v \n===================\n", ok, value)

	   	bt.Set("hello", 10231)
	   	fmt.Printf("SET key : hello value : %v \n===================\n", value)

	   	ok, value = bt.Search("hello")
	   	fmt.Printf("found hello : %v value : %v \n===================\n", ok, value)

	   	bt.Set("hfffd", 9932)
	   	ok, value = bt.Search("hfffd")
	   	fmt.Printf("found : %v value : %v \n===================\n", ok, value)

	   	// for i:=0;i<50;i++{
	   	//     bt.Set(fmt.Sprintf("%v",i),9932)
	   	//  }
	   	for i := 0; i < 200000; i++ {
	   		bt.Set(fmt.Sprintf("%v", i), uint32(i))
	   	}
	   	bt.Set("fmt", 444)
	       //bt.Display()

	       fmt.Printf("build b+tree over \n===================\n")

	      // bt.Display()

	       for i:=25000;i>=0;i-- {
	           ok, value = bt.Search(fmt.Sprintf("%v", rand.Intn(200000)))
	           if !ok{
	               fmt.Printf("notfound : %v value : %v \n===================\n", i, value)
	           }

	       }


	       ok, value = bt.Search("248")
	       //ok, value = bt.Search("32")
	   	//fmt.Printf("found : %v value : %v \n===================\n", ok, value)

	   	// ttttt()
	   	//bt.print()
	   	//bt.display()

	   	//bt.DisplayTree()

	   	//verifyTree(bt, testCount, t)
	*/
}

func TestLoadTree(t *testing.T) {

	db := NewBTDB("bp.tree")
	ok, value := db.Search("test", "hello")
	fmt.Printf("found hello : %v value : %v \n===================\n", ok, value)

	db.AddBTree("test")
	//db.Set("test", "hello", 3456)
	ok, value = db.Search("test", "hello")
	fmt.Printf("found hello : %v value : %v \n===================\n", ok, value)
	start := time.Now()
	for i := 0; i < 100000; i++ {
		//fmt.Printf("%04d\n", i)
		db.Set("test", fmt.Sprintf("%08d", rand.Intn(20000000)), uint64(i))
	}
	//db.Set("test", "fmt", 444)
	//bt.Display()

	fmt.Printf("build b+tree over cost %v \n===================\n", time.Now().Sub(start))
	start = time.Now()
	// bt.Display()

	for i := 100000; i >= 0; i-- {
		ii := fmt.Sprintf("%08d", rand.Intn(2000))
		ok, value = db.Search("test", ii)
		if !ok {
			//fmt.Printf("notfound : %v value : %v \n===================\n", ii, value)
		}

	}
	fmt.Printf("query b+tree over cost %v\n===================\n", time.Now().Sub(start))
/*
	found, ranges := db.Range("test", "", "09999900")

	if found {
		fmt.Printf(">>>> %v\n", ranges)
	}

	key, value1, pgnum, idx, ok := db.GetFristKV("test")
	fmt.Printf("key:%v value:%v index:%v pagenum:%v ok:%v\n", key, value1, idx, pgnum, ok)

	for ok {
		key, value1, pgnum, idx, ok = db.GetNextKV("test", pgnum, idx)
		fmt.Printf("key:%v value:%v index:%v pagenum:%v ok:%v\n", key, value1, idx, pgnum, ok)

	}
*/
}
/*
func TestMergeTree(t *testing.T) {
	//start := time.Now()
	db := NewBTDB("bp.tree")
	db.AddBTree("AAA")
	for i := 0; i < 10; i++ {
		db.Set("AAA", fmt.Sprintf("%04d", i), uint64(i))
	}
	db.AddBTree("BBB")
	for i := 0; i < 15; i++ {
		db.Set("BBB", fmt.Sprintf("%04d", i), uint64(i))
	}

	db.AddBTree("CCC")
	for i := 0; i < 20; i++ {
		db.Set("CCC", fmt.Sprintf("%04d", rand.Intn(100)), uint64(i))
	}

	key1, value1, pgnum1, idx1, ok1 := db.GetFristKV("AAA")
	fmt.Printf("key:%v value:%v index:%v pagenum:%v ok:%v\n", key1, value1, idx1, pgnum1, ok1)

	key2, value2, pgnum2, idx2, ok2 := db.GetFristKV("BBB")
	fmt.Printf("key:%v value:%v index:%v pagenum:%v ok:%v\n", key2, value2, idx2, pgnum2, ok2)

	key3, value3, pgnum3, idx3, ok3 := db.GetFristKV("CCC")
	fmt.Printf("key:%v value:%v index:%v pagenum:%v ok:%v\n", key3, value3, idx3, pgnum3, ok3)

	for ;ok1 || ok2 || ok3; {

		if ok1 {
			key1, value1, pgnum1, idx1, ok1 = db.GetNextKV("AAA", pgnum1, idx1)
			fmt.Printf("AAA:::key:%v value:%v index:%v pagenum:%v ok:%v\n", key1, value1, idx1, pgnum1, ok1)

		}

		if ok2 {
			key2, value2, pgnum2, idx2, ok2 = db.GetNextKV("BBB", pgnum2, idx2)
			fmt.Printf("BBB:::key:%v value:%v index:%v pagenum:%v ok:%v\n", key2, value2, pgnum2, idx2, ok2)
		}
		if ok3 {
			key3, value3, pgnum3, idx3, ok3 = db.GetNextKV("CCC", pgnum3, idx3)
			fmt.Printf("CCC:::key:%v value:%v index:%v pagenum:%v ok:%v\n", key3, value3, pgnum3, idx3, ok3)
		}

	}

}
*/