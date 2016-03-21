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

    fmt.Printf("element len:%v\n",int(unsafe.Sizeof(element{})))
	db := NewBTDB("bp.tree")
	ok, value := db.Search("test", "hello")
	fmt.Printf("found hello : %v value : %v \n===================\n", ok, value)
    for i:=0;i<30;i++{
        db.AddBTree(fmt.Sprintf("%v",i))
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
	for i := 0; i < 200000; i++ {
        //fmt.Printf("%04d\n", i)
		db.Set("test", fmt.Sprintf("%08d", rand.Intn(20000000)), uint64(i))
	}
	//db.Set("test", "fmt", 444)
	//bt.Display()

	fmt.Printf("build b+tree over cost %v \n===================\n",time.Now().Sub(start))
    start= time.Now()
	// bt.Display()

	for i := 10; i >= 0; i-- {
        ii:=fmt.Sprintf("%08d", rand.Intn(200000))
		ok, value = db.Search("test", ii)
		if !ok {
			fmt.Printf("notfound : %v value : %v \n===================\n", ii, value)
		}

	}
    fmt.Printf("query b+tree over cost %v\n===================\n",time.Now().Sub(start))
    
    found,ranges := db.Range("test","","09999900")
    
    if found {
        fmt.Printf(">>>> %v\n",ranges)
    }
   
}
