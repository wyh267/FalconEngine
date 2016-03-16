package tree

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestNewTree(t *testing.T) {

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
}
