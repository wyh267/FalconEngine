/*****************************************************************************
 *  file name : btree.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : b+tree
 *
******************************************************************************/

package tree

import (
	"errors"
	"fmt"
	"os"
	"sort"
	//"sort"
	"syscall"
	"unsafe"
)

const (
	tmeta     uint8 = 1
	tinterior uint8 = 2
	tleaf     uint8 = 3
)

const pagesize int64 = 1024 * 4

const pageheadOffset int64 = int64(unsafe.Offsetof(((*page)(nil)).elementsptr))
const elementSize int64 = int64(unsafe.Sizeof(element{}))
const maxitems int64 = 100
const pageheaadlen int64 = pageheadOffset + elementSize*maxitems //4*8 + 24*100
const maxkeylen uint32 = 32
type element struct {
	//key   string
	bkey   [maxkeylen]byte
	ksize uint32
	value uint32
}

func (e *element) key() string { return string(e.bkey[:e.ksize])}
func (e *element) setkv(key string,value uint32){ 
    if len(key) > int(maxkeylen) {
        copy(e.bkey[:],[]byte(key)[:maxkeylen])
        e.ksize=maxkeylen
        e.value=value
        return
    }
    copy(e.bkey[:len(key)],[]byte(key)[:])
    e.ksize=uint32(len(key))
    e.value=value
    return 
}

type sorteles []element

func (s sorteles) Len() int           { return len(s) }
func (s sorteles) Swap(i, j int)      { s[i], s[j] = s[j], s[i]}
func (s sorteles) Less(i, j int) bool { return s[i].key() > s[j].key() }
/*
     //s[i], s[j] = s[j], s[i]    
     jpos:=(uint32(uintptr(unsafe.Pointer(&s[i])))+s[i].pos)-uint32(uintptr(unsafe.Pointer(&s[j])))  
     ipos:=(uint32(uintptr(unsafe.Pointer(&s[j])))+s[j].pos)-uint32(uintptr(unsafe.Pointer(&s[i])))
     //fmt.Printf("ipos :%v %v %v %v jpos :%v  %v %v %v \n",s[i].ksize,s[i].value,s[i].pos,ipos,s[j].ksize,s[j].value,s[j].pos,jpos)
     s[j].pos=jpos
     s[i].pos=ipos
     s[j].ksize,s[i].ksize = s[i].ksize,s[j].ksize
     s[j].value,s[i].value = s[i].value,s[j].value
}


func (s sorteles) Less(i, j int) bool { 
    //fmt.Printf("i : %v j : %v\n",i,j)
    return s[i].key() > s[j].key() 

}
*/
// page description:页的基本单位
type page struct {
	curid       uint32
	preid       uint32
	nextid      uint32
	parentpg    uint32
	pgtype      uint8
	count       uint32
	used        uint32
	elementsptr uintptr
}

type pagestack struct {
	page  *page
	index int
}



func (p *page) display(bt *btree){
    
    elements := p.getElements()
    fmt.Printf("[INFO]==>display :: ELEMENTS ")
    for i:=range elements[:p.count] {
        e:=&elements[i]
        
        if p.pgtype == tinterior {
            fmt.Printf("::: key[%v]  %v\n", e.key(), e.value)
            child:=bt.getpage(e.value)
            child.display(bt)
        }else{
            fmt.Printf("::: key[%v]  %v\t", e.key(), e.value)
        }
    }
    fmt.Println()
    
    
}


func (p *page) set(key string, value uint32, bt *btree) bool {

	stack := make([]pagestack, 0)

	found, _, idx, err := p.search(key, &stack, bt)

	if err != nil {
		fmt.Printf("[ERROR] can not set key value %v %v %v\n", found, value, idx)
		return false
	}

	//直接更新
	if found {
		//fmt.Printf("FOUND %v %v %v \n", key, v, idx)
		stack[len(stack)-1].page.getElement(idx).value = value
		return true
	}

	page := stack[len(stack)-1].page
	index := stack[len(stack)-1].index
	p1, p2, err := page.insertleaf(index, key, value,bt)
    if err!=nil {
        return false
    }

	for idx := len(stack) - 2; idx >= 0; idx-- {

		page := stack[idx].page
		index := stack[idx].index
		p1, p2, _ = page.interiorinsert(index, p1, p2,bt)
		//fmt.Printf("STACK PAGE :::: %v index: %v\n", unsafe.Pointer(stack[idx].page), stack[idx].index)
	}
    
    if p1!=nil && p2!=nil {

        newroot:=bt.newpage(nil,nil,tinterior)
        newroot.count=2
        rooteles := newroot.getElements()
        rooteles[0].setkv(p1.getElement(int(p1.count-1)).key(),p1.curid)
        rooteles[1].setkv("",p2.curid)
        bt.root=newroot
        bt.rootpg=newroot.curid
        fmt.Printf("new root...%v\n",newroot.curid)
    }

	return true

}

func (p *page) interiorinsert(index int, child1, child2 *page,bt *btree) (*page, *page, error) {

	
    
    if child1!=nil && child2 == nil {
        
        //if p.count < uint32(maxitems) {
            elements := p.getElements()
            child1node:=child1.getElement(int(child1.count-1))
            elements[index].value=child1.curid
            if elements[index].key()!="" {
                //fmt.Printf("NNNNNNNNNN:%v %v\n",elements[index].key(),child1node.key())
                elements[index].setkv(child1node.key(), child1.curid ) //uintptr(unsafe.Pointer(p)))
            }
            //sort.Sort(sorteles(elements[:p.count]))
            return p,nil,nil
        //}
        
        
        
    }
    
    
   
    
	if child2 != nil && child1 != nil {
       
        elements := p.getElements()
        
       
            
      
        child1node:=child1.getElement(int(child1.count-1))
        
        elements[p.count].setkv(child1node.key(), child1.curid) //uintptr(unsafe.Pointer(p)))
		p.count++
        
        child2node:=child2.getElement(int(child2.count-1))
        elements[index].value=child2.curid
        if elements[index].key()!="" {
            //fmt.Printf("YYYYYY:%v %v\n",elements[index].key(),child2node.key())
             elements[index].setkv(child2node.key(), child2.curid) //uintptr(unsafe.Pointer(p)))
        }
        
        sort.Sort(sorteles(elements[:p.count]))
        
        //fmt.Printf("[INFO]==>INSERT :: INTERIOR ELEMENTS child1 %v child2 %v parent :%v",child1.curid,child2.curid,p.curid)
       // for i:=range elements[:p.count] {
       //     e:=&elements[i]
            //fmt.Printf("::: key[%v]  %v\t", e.key(), e.value)
       // }
        //fmt.Println()
        if p.count < uint32(maxitems) {
		  return p, nil, nil
        }
        
        //fmt.Printf("inter is full\n")
        parent:=bt.getpage(p.parentpg)
        newpage:=bt.newpage(parent,p,tinterior)
        newpage.count=0
        ii:=0
        for i:=int(p.count)/2;i<int(p.count);i++{
            pele:=p.getElement(i)
            ele:=newpage.getElement(ii)
            ele.setkv(pele.key(),pele.value)
            newpage.count++ 
            ii++
            
        }
        p.count = p.count/2
        return p,newpage,nil
              
        
	}
    
	return p, nil, nil

}


func makeBufferPage(src *page) *page {
    srcbuf:=(*[0xFFFFFF]byte)(unsafe.Pointer(src))
    buf:=make([]byte,pagesize)
    copy(buf,srcbuf[:pagesize])
    return (*page)(unsafe.Pointer(&buf[0]))
    
}

func (p *page) split(key string,value uint32,bt *btree) (*page,*page,error) {
    
    elements := p.getElements()
    elements[p.count].setkv(key, value) //uintptr(unsafe.Pointer(p)))
    p.count++
    //fmt.Printf("[INFO]==>INSERT [used : %v] :: LEAF ELEMENTS ",p.used)
    sort.Sort(sorteles(elements[:p.count]))
    
    parent:=bt.getpage(p.parentpg)
    newpage:=bt.newpage(parent,p,tleaf)
    
    ii:=0
    for i:=int(p.count)/2;i<int(p.count);i++{
        pele:=p.getElement(i)
        ele:=newpage.getElement(ii)
        ele.setkv(pele.key(),pele.value)
        newpage.count++ 
        ii++
        
    }
    p.count = p.count/2
    
    return p,newpage,nil
    
    
}


func (p *page) insertleaf(index int, key string, value uint32,bt *btree) (*page, *page, error) {

	if p.pgtype == tleaf {
        
        if  p.count == uint32(maxitems) {
            //fmt.Printf("[ERROR] ... page is  full split pages\n")
            return p.split(key,value,bt)//nil,nil,errors.New("page is full")
        }
        
		elements := p.getElements()
        elements[p.count].setkv(key, value) //uintptr(unsafe.Pointer(p)))
		p.count++
		sort.Sort(sorteles(elements[:p.count]))
        /*
		for i:=range elements[:p.count] {
            e:=&elements[i]
            fmt.Printf("::: %v  %v\t", e.key(), e.value)
        }
        fmt.Println()
        */
        //fmt.Printf("[INFO]==>INSERT :: LEAF ELEMENTS ::: %v  %v %v\n", elements[:p.count], p.count, p.used)
		return p, nil, nil
	} 
	return nil, nil, errors.New("insert error")

}

func (p *page) search(key string, stack *[]pagestack, bt *btree) (bool, uint32, int, error) {

	if p.pgtype == tleaf {
		if p.count == 0 {
			*stack = append(*stack, pagestack{page: p, index: 0})
			//fmt.Printf("[INFO]==>SEARCH :: leaf empty , search key [%v] found !! \n", key)
			//p.count++
			return false, 0, 0, nil
		}

		//循环查找
		elements := p.getElements()
        c:=func(i int) bool {
            return elements[i].key() <= key 
        }
        idx := sort.Search(int(p.count),c) 
        if idx<int(p.count){
            if elements[idx].key() == key {
                *stack = append(*stack, pagestack{page: p, index: idx})
				return true, elements[idx].value, idx, nil
            }
            *stack = append(*stack, pagestack{page: p, index: idx})
		    return false, elements[idx].value, idx, nil
        }
        
        /*
		for idx := 0; idx < int(p.count); idx++ {
			e := &elements[idx]

			if key == e.key() {
				//fmt.Printf("[INFO]==>SEARCH :: leaf node key :[%v] , search key [%v] found !! \n", e.key(), key)
				*stack = append(*stack, pagestack{page: p, index: idx})
				return true, e.value, idx, nil
			}
			if key > e.key() {
				//fmt.Printf("[INFO]==>SEARCH :: leaf node key :[%v] , search key [%v] append to index[%v] !! \n", e.key(), key, idx)
				*stack = append(*stack, pagestack{page: p, index: idx})
				return false, e.value, idx, nil
			}
		}
        */
		*stack = append(*stack, pagestack{page: p, index: 0})
		return false, 0, 0, nil //errors.New("found error")
	} else if p.pgtype == tinterior {
		if p.count == 0 {
			*stack = append(*stack, pagestack{page: p, index: 0})
			return false, 0, -1, errors.New("ERROR")
		}

		//循环查找
		elements := p.getElements()
        
        c:=func(i int) bool {
            return elements[i].key() <= key 
        }
        idx := sort.Search(int(p.count),c) 
        if idx<int(p.count){
            *stack = append(*stack, pagestack{page: p, index: idx})
            sub := bt.getpage(elements[idx].value)
            return sub.search(key, stack, bt)
        }
        
		//没有找到,需要添加
		*stack = append(*stack, pagestack{page: p, index: -1})
		return false, 0, -1, errors.New("found error")
	}
	fmt.Printf("[ERROR]==>SEARCH :: b+tree error \n")
	return false, 0, -1, errors.New("ERROR")

}

func (p *page) getElements() []element {

	return ((*[0xFFFF]element)(unsafe.Pointer(&p.elementsptr)))[:]
}

func (p *page) getElement(index int) *element {

	return &((*[0xFFFF]element)(unsafe.Pointer(&p.elementsptr)))[index]
}

// btree function description : b+树
type btree struct {
	filename  string
	rootpg    uint32
	root      *page
	mmapbytes []byte
	maxpgid   uint32
	fd        *os.File
}

func NewEmptyBTree(filename string) *btree {

	fmt.Printf("headoffset : %v \n", pageheadOffset)
	fmt.Printf("elementSize: %v \n", elementSize)
	fmt.Printf("pageheaadlen: %v \n", pageheaadlen)

	file_create_mode := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	this := &btree{filename: filename, rootpg: 0, maxpgid: 1}

	file_create_mode = os.O_RDWR | os.O_CREATE | os.O_TRUNC
	f, err := os.OpenFile(filename, file_create_mode, 0664)
	if err != nil {
		return nil
	}
	this.fd = f
	//defer f.Close()
	syscall.Ftruncate(int(f.Fd()), pagesize*2)
	this.mmapbytes, err = syscall.Mmap(int(f.Fd()), 0, int(pagesize*2), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return nil
	}
	if this.init() != nil {
		return nil
	}
	return this

}

func (bt *btree) init() error {

	bt.root = bt.newpage(nil, nil, tinterior)
	bt.rootpg = bt.root.curid
	leaf := bt.newpage(bt.root, nil, tleaf)
	ele := bt.root.getElement(0)
	ele.value = leaf.curid

	fmt.Printf("LEAF COUNT INIT:%v\n", leaf.count)
	//leafele := leaf.getElement(0)
	//leafele.key = "hello"
	//leafele.value = 33
	//leaf.count++
	return nil

}

func (bt *btree) Set(key string, value uint32) bool {
    //fmt.Printf("new root id ...%v %v\n",bt.root.curid,bt.root.getElement(int(bt.root.count-1)).key())
	return bt.root.set(key, value, bt)

}

func (bt *btree) checkmmap() error {
	if int(int64(bt.maxpgid)*pagesize) >= len(bt.mmapbytes) {
		//bp.maxpgid++
		err := syscall.Ftruncate(int(bt.fd.Fd()), int64(bt.maxpgid+1)*pagesize)
		if err != nil {
			fmt.Printf("ftruncate error : %v\n", err)
			return err
		}
		syscall.Munmap(bt.mmapbytes)
		bt.mmapbytes, err = syscall.Mmap(int(bt.fd.Fd()), 0, int(int64(bt.maxpgid+1)*pagesize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

		if err != nil {
			fmt.Printf("MAPPING ERROR  %v \n", err)
			return err
		}

	}
	return nil
}

func (bt *btree) newpage(parent, pre *page, pagetype uint8) *page {

	if bt.checkmmap() != nil {
        fmt.Printf("check error \n")
		return nil
	}

	lpage := (*page)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.maxpgid) * pagesize)]))
	lpage.curid = bt.maxpgid
	lpage.pgtype = pagetype
	lpage.nextid = 0
	lpage.preid = 0
	if pagetype == tinterior {
		lpage.count = 1
		ele := (*[0xFFFF]element)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.maxpgid)*pagesize + pageheadOffset)]))
		lpage.used = uint32(pageheaadlen)
		ele[0].setkv("", 0)
		lpage.elementsptr = uintptr(unsafe.Pointer(ele))

	} else {
		lpage.count = 0
		ele := (*[0xFFFF]element)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.maxpgid)*pagesize + pageheadOffset)]))
		//ele[0].key="hello"
		//ele[0].value=345
		lpage.elementsptr = uintptr(unsafe.Pointer(ele))
		lpage.used = uint32(pageheaadlen)
	}

	if parent != nil {
		lpage.parentpg = parent.curid
	} else {
		lpage.parentpg = 0
	}

	if pre != nil {
		lpage.nextid = pre.nextid
		pre.nextid = lpage.curid
		lpage.preid = pre.curid
	}

	bt.maxpgid++
	return lpage
}

func (bt *btree) getpage(pgid uint32) *page {

	return (*page)(unsafe.Pointer(&bt.mmapbytes[(int64(pgid) * pagesize)]))

}

func (bt *btree) Search(key string) (bool, uint32) {

	stack := make([]pagestack, 0)
	ok, value, _, _ := bt.root.search(key, &stack, bt)

	for idx := len(stack) - 1; idx >= 0; idx-- {

		//fmt.Printf("STACK PAGE :::: %v index: %v\n", unsafe.Pointer(stack[idx].page), stack[idx].index)
	}

	return ok, value
}


func (bt *btree) Display(){
    
    bt.root.display(bt)
    
}

type eee struct {
	key []byte
	// pos     uint32
	len   uint8
	value uint32
}

func ttttt() {

	e := eee{value: 20}
	copy(e.key[:20], []byte("he"))
	fmt.Printf("%v %v\n", unsafe.Sizeof(e), e.key)

}

/*

type pages []*page

type nodes []*node

type inode struct {
	pgid  uint32
	key   string
	value uint32
}

type inodes []inode

type node struct {
	nodetype uint8
	pageid   uint32
	parent   *node
	key      string
	children nodes
	inodes   inodes
	pgids    pages
}

func (n *node) getKey(index int) string {
	if n.nodetype == tleaf && index < len(n.inodes) {
		return n.inodes[index].key
	}
	if n.nodetype == tinterior && index < len(n.children) && n.children[index] != nil {
		return n.children[index].key
	}

	return ""
}

func (n *node) display(deep int) {

	for i := 0; i < deep; i++ {
		fmt.Printf("\t")
	}

	if n.nodetype == tleaf {
		fmt.Printf("[%v] ", unsafe.Pointer(n))
		for _, in := range n.inodes {

			fmt.Printf(" leafkey:%v == %v \t", in.key, in.value)

		}
	}

	if n.nodetype == tinterior {
		fmt.Printf("[%v] ", unsafe.Pointer(n))
		for _, in := range n.children {

			fmt.Printf(" interkey:%v == %v \n", in.key, unsafe.Pointer(in))
			in.display(deep + 1)

		}
	}
	fmt.Println()

}

func (n *node) put(key string, value uint32, bt *btree) bool {

	stack := make([]*node, 0)

	ok, v, index, err := n.search(key, &stack)

	if err != nil {
		fmt.Printf("[ERROR] node is nil...\n")
		return false
	}

	if ok && v != value {
		stack[len(stack)-1].inodes[index].value = value
		fmt.Printf("[INFO] update value[%v] ok...\n", value)
		return ok
	}

	//没有key ，需要添加key,value
	if !ok {
		for idx := len(stack) - 1; idx >= 0; idx-- {
			n := stack[idx]
			fmt.Printf("STACK::::%v\n", unsafe.Pointer(stack[idx]))
			n.add(key, value, bt)

		}

	}

	return true
}

func (n *node) addpage(bt *btree, pre *page, ptype uint8) *page {

	if bt.checkmmap() != nil {
		return nil
	}

	lpage := (*page)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.maxpgid) * pagesize)]))
	lpage.curid = bt.maxpgid
	lpage.pgtype = tinterior
	lpage.nextid = 0
	lpage.preid = pre.curid
	lpage.ismaster = false
	lpage.parentpg = pre.parentpg
	pre.nextid = lpage.curid

	if ptype == tleaf {
		lpage.pgtype = tleaf
		leafp := (*leafpage)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.maxpgid)*pagesize + pageheaadlen)]))
		leafp.count = 0
		lpage.ptr = uintptr(unsafe.Pointer(leafp))
	} else {
		lpage.pgtype = tinterior
		interp := (*interpage)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.maxpgid)*pagesize + pageheaadlen)]))
		interp.count = 0
		lpage.ptr = uintptr(unsafe.Pointer(interp))
	}

	bt.maxpgid++
	return lpage

}

func (n *node) add(key string, value uint32, bt *btree) error {

	if n.nodetype == tleaf {
		fmt.Printf("add : %v %v leaf\n", key, value)
		lpg := (*leafpage)(unsafe.Pointer(n.pgids[len(n.pgids)-1].ptr))
		if lpg.count == 0 {
			lpg.leafeles = append(lpg.leafeles, leafelement{key: key, value: value})
			lpg.count++
			return nil
		} else {
			lpage := n.addpage(bt, n.pgids[len(n.pgids)-1], tleaf)
			lpg := (*leafpage)(unsafe.Pointer(lpage.ptr))
			lpg.leafeles = append(lpg.leafeles, leafelement{key: key, value: value})
			lpg.count++
			n.pgids = append(n.pgids, lpage)
			return nil
		}
	} else {
		fmt.Printf("add : %v %v inter\n", key, value)

	}

	return nil

}

func (n *node) search(key string, stack *[]*node) (bool, uint32, int, error) {

	if n.nodetype == tleaf {
		//循环查找
		for idx, in := range n.pgids {
			lpg := (*leafpage)(unsafe.Pointer(in.ptr))
			if lpg.count == 0 {
				*stack = append(*stack, n)
				return false, 0, -1, nil
			}
			if key == lpg.leafeles[0].key {
				*stack = append(*stack, n)
				return true, lpg.leafeles[0].value, idx, nil
			}
		}
		*stack = append(*stack, n)
		return false, 0, -1, nil
	}

	if n.nodetype == tinterior {

		for _, in := range n.children {

			if key >= in.key {
				fmt.Printf("less key : %v , %v \n", key, in.key)
				*stack = append(*stack, n)
				return in.search(key, stack)
			}
		}
		fmt.Printf("tinterior\n")
	}
	fmt.Printf("ERROR\n")
	return false, 0, -1, errors.New("ERROR")

}

func (bt *btree) search(key string) (bool, uint32, int, error) {

	stack := make([]*node, 0)

	a, b, c, err := bt.root.search(key, &stack)

	for idx := len(stack) - 1; idx >= 0; idx-- {
		fmt.Printf("STACK::::%v\n", unsafe.Pointer(stack[idx]))
	}

	return a, b, c, err

}

func (bt *btree) display() {

	bt.root.display(0)

}

func (bt *btree) put(key string, value uint32) bool {

	return bt.root.put(key, value, bt)

}

func (bt *btree) newleafnode(parent *node) *node {

	if bt.checkmmap() != nil {
		return nil
	}

	lpage := (*page)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.maxpgid) * pagesize)]))
	lpage.curid = bt.maxpgid
	lpage.pgtype = tleaf
	lpage.nextid = 0
	lpage.preid = 0
	lpage.ismaster = true
	if parent != nil {
		lpage.parentpg = parent.pageid
	} else {
		lpage.parentpg = 0
	}

	leafp := (*leafpage)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.maxpgid)*pagesize + pageheaadlen)]))
	leafp.count = 0
	lpage.ptr = uintptr(unsafe.Pointer(leafp))

	leafnode := &node{nodetype: tleaf, pageid: bt.maxpgid, parent: parent, pgids: make(pages, 0)}
	leafnode.pgids = append(leafnode.pgids, lpage)

	if parent != nil {
		parent.children = append(parent.children, leafnode)
	}

	bt.maxpgid++
	return leafnode

}

func (bt *btree) newinternode(parent *node) *node {

	if bt.checkmmap() != nil {
		return nil
	}

	lpage := (*page)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.maxpgid) * pagesize)]))
	lpage.curid = bt.maxpgid
	lpage.pgtype = tinterior
	lpage.nextid = 0
	lpage.preid = 0
	lpage.ismaster = true
	if parent != nil {
		lpage.parentpg = parent.pageid

	} else {
		lpage.parentpg = 0
	}

	interp := (*interpage)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.maxpgid)*pagesize + pageheaadlen)]))
	interp.count = 0
	lpage.ptr = uintptr(unsafe.Pointer(interp))

	intnode := &node{nodetype: tinterior, pageid: bt.maxpgid, parent: parent, pgids: make(pages, 0)}
	intnode.pgids = append(intnode.pgids, lpage)

	if parent != nil {
		parent.children = append(parent.children, intnode)
	}

	bt.maxpgid++

	return intnode

}
*/
