package main

import (
	"container/heap"
	"fmt"
)

// An IntHeap is a min-heap of ints.
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
    //x:=old[0]
    //*h = old[1:n]
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// This example inserts several ints into an IntHeap, checks the minimum,
// and removes them in order of priority.
func main() {
	h := &IntHeap{100, 16, 4, 8, 70, 2, 36, 22, 5, 12}

	fmt.Println("\nHeap:")
	heap.Init(h)
    //heap.Push(h,33)
    //for h.Len() > 0 {
	//	fmt.Printf("%d ", heap.Pop(h))
	//}

	//for(Pop)依次输出最小值,则相当于执行了HeapSort
	fmt.Println("\nPush(h, 3),然后输出堆看看:")
    for i:=0;i<40;i++{
        //fmt.Printf("i : %v min:%v\n",i,(*h)[0])
        //if i > (*h)[0] {
        heap.Push(h, i)
        heap.Pop(h)
        //}
    }
	
	for h.Len() > 0 {
		fmt.Printf("%d ", heap.Pop(h))
	}

}
