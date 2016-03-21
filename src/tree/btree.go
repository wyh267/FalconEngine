/*****************************************************************************
 *  file name : btree.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : b+tree
 *
******************************************************************************/

package tree

//#include <sys/mman.h>
//import "C"
import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	//"strconv"
	"syscall"
	"unsafe"
)

const (
	tmeta     uint8 = 1
	tinterior uint8 = 2
	tleaf     uint8 = 3
)

const pagesize int64 = 1024 * 4 * 2

const pageheadOffset int64 = int64(unsafe.Offsetof(((*page)(nil)).elementsptr))
const elementSize int64 = int64(unsafe.Sizeof(element{}))
const maxitems int64 = 100
const pageheaadlen int64 = pageheadOffset + elementSize*maxitems //4*8 + 24*100
const maxkeylen uint32 = 64

type element struct {
	bkey  [maxkeylen]byte
	ksize uint32
	value uint64
}

func (e *element) key() string { return string(e.bkey[:e.ksize]) }
func (e *element) setkv(key string, value uint64) {
	if len(key) > int(maxkeylen) {
		copy(e.bkey[:], []byte(key)[:maxkeylen])
		e.ksize = maxkeylen
		e.value = value
		return
	}
	copy(e.bkey[:len(key)], []byte(key)[:])
	e.ksize = uint32(len(key))
	e.value = value
	return
}

type sorteles []element

func (s sorteles) Len() int           { return len(s) }
func (s sorteles) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s sorteles) Less(i, j int) bool { return s[i].key() > s[j].key() }

//
//	ii, _ := strconv.Atoi(s[i].key())
//	jj, _ := strconv.Atoi(s[j].key())
//	return ii > jj
//}

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
	pageid uint32
	index  int
}

func (p *page) display(bt *btree) {

	elements := p.getElements()
	fmt.Printf("[INFO]==>display :: ELEMENTS ")
	for i := range elements[:p.count] {
		e := &elements[i]

		if p.pgtype == tinterior {
			fmt.Printf("::: key[%v]  %v\n", e.key(), e.value)
			child := bt.getpage(uint32(e.value))
			child.display(bt)
		} else {
			fmt.Printf("::: key[%v]  %v\t", e.key(), e.value)
		}
	}
	fmt.Println()

}

func (p *page) set(key string, value uint64, bt *btree) bool {

	stack := make([]pagestack, 0)

	found, _, idx, err := p.search(key, &stack, bt)

	if err != nil {
		fmt.Printf("[ERROR] can not set key value %v %v %v\n", found, value, idx)
		return false
	}

	//直接更新
	if found {
		bt.getpage(stack[len(stack)-1].pageid).getElement(idx).value = value
		return true
	}

	page := bt.getpage(stack[len(stack)-1].pageid)
	index := stack[len(stack)-1].index
	pg1, pg2, err := page.insertleaf(index, key, value, bt)
	if err != nil {
		return false
	}

	for idx := len(stack) - 2; idx >= 0; idx-- {

		page := bt.getpage(stack[idx].pageid)
		index := stack[idx].index
		pg1, pg2, _ = page.interiorinsert(index, pg1, pg2, bt)
	}

	if pg1 != 0 && pg2 != 0 {

		newroot, _, _ := bt.newpage(0, 0, tinterior)
		p1 := bt.getpage(pg1)
		p2 := bt.getpage(pg2)
		newroot.count = 2
		rooteles := newroot.getElements()
		rooteles[0].setkv(p1.getElement(int(p1.count-1)).key(), uint64(p1.curid))
		rooteles[1].setkv("", uint64(p2.curid))
		bt.root = newroot
		bt.db.setrootid(bt.name)
		bt.rootpgid = newroot.curid
		//bt.rootpg = newroot.curid
		fmt.Printf("new root...%v\n", newroot.curid)
	}

	return true

}

func (p *page) interiorinsert(index int, pg1, pg2 uint32, bt *btree) (uint32, uint32, error) {

	if pg1 != 0 && pg2 == 0 {
		child1 := bt.getpage(pg1)
		elements := p.getElements()
		child1node := child1.getElement(int(child1.count - 1))
		elements[index].value = uint64(child1.curid)
		if elements[index].key() != "" {
			elements[index].setkv(child1node.key(), uint64(child1.curid)) //uintptr(unsafe.Pointer(p)))
		}
		return p.curid, 0, nil
	}

	if pg1 != 0 && pg2 != 0 {
		child1 := bt.getpage(pg1)
		child2 := bt.getpage(pg2)
		elements := p.getElements()
		child1node := child1.getElement(int(child1.count - 1))
		elements[p.count].setkv(child1node.key(), uint64(child1.curid)) //uintptr(unsafe.Pointer(p)))
		p.count++

		child2node := child2.getElement(int(child2.count - 1))
		elements[index].value = uint64(child2.curid)
		if elements[index].key() != "" {
			elements[index].setkv(child2node.key(), uint64(child2.curid)) //uintptr(unsafe.Pointer(p)))
		}

		sort.Sort(sorteles(elements[:p.count]))

		if p.count < uint32(maxitems) {
			return p.curid, 0, nil
		}

		/////////parent := bt.getpage(p.parentpg)
		var newpage *page
		newpage, _, p = bt.newpage(0, p.curid, tinterior)
		newpage.count = 0
		ii := 0
		for i := int(p.count) / 2; i < int(p.count); i++ {
			pele := p.getElement(i)
			ele := newpage.getElement(ii)
			ele.setkv(pele.key(), pele.value)
			newpage.count++
			ii++

		}
		p.count = p.count / 2
		return p.curid, newpage.curid, nil

	}

	return p.curid, 0, nil

}

func makeBufferPage(src *page) *page {
	srcbuf := (*[0xFFFFFF]byte)(unsafe.Pointer(src))
	buf := make([]byte, pagesize)
	copy(buf, srcbuf[:pagesize])
	return (*page)(unsafe.Pointer(&buf[0]))

}

func (p *page) split(key string, value uint64, bt *btree) (uint32, uint32, error) {

	elements := p.getElements()
	elements[p.count].setkv(key, value) //uintptr(unsafe.Pointer(p)))
	p.count++
	sort.Sort(sorteles(elements[:p.count]))

	///////////parent := bt.getpage(p.parentpg)
	var newpage *page
	newpage, _, p = bt.newpage(0, p.curid, tleaf)

	ii := 0
	for i := int(p.count) / 2; i < int(p.count); i++ {
		pele := p.getElement(i)
		ele := newpage.getElement(ii)
		ele.setkv(pele.key(), pele.value)
		newpage.count++
		ii++

	}
	p.count = p.count / 2

	return p.curid, newpage.curid, nil

}

func (p *page) insertleaf(index int, key string, value uint64, bt *btree) (uint32, uint32, error) {

	if p.pgtype == tleaf {

		if p.count == uint32(maxitems) {
			return p.split(key, value, bt) //nil,nil,errors.New("page is full")
		}

		elements := p.getElements()
		elements[p.count].setkv(key, value) //uintptr(unsafe.Pointer(p)))
		p.count++
		sort.Sort(sorteles(elements[:p.count]))
		return p.curid, 0, nil
	}
	return 0, 0, errors.New("insert error")

}

func (p *page) search(key string, stack *[]pagestack, bt *btree) (bool, uint64, int, error) {

	if p.pgtype == tleaf {
		if p.count == 0 {
			*stack = append(*stack, pagestack{pageid: p.curid, index: 0})
			return false, 0, 0, nil
		}

		//循环查找
		elements := p.getElements()
		c := func(i int) bool {
			// ee,_:=strconv.Atoi(elements[i].key())
			//kk,_:=strconv.Atoi(key)
			//return ee<=kk//elements[i].key() <= key
			return elements[i].key() <= key
		}
		idx := sort.Search(int(p.count), c)
		if idx < int(p.count) {
			if elements[idx].key() == key {
				//fmt.Printf("found : %v %v\n",key,elements[idx].value)
				*stack = append(*stack, pagestack{pageid: p.curid, index: idx})
				return true, elements[idx].value, idx, nil
			}
			*stack = append(*stack, pagestack{pageid: p.curid, index: idx})
			return false, elements[idx].value, idx, nil
		}

		*stack = append(*stack, pagestack{pageid: p.curid, index: 0})
		return false, 0, 0, nil //errors.New("found error")
	} else if p.pgtype == tinterior {
		if p.count == 0 {
			*stack = append(*stack, pagestack{pageid: p.curid, index: 0})
			return false, 0, -1, errors.New("ERROR")
		}

		//循环查找
		elements := p.getElements()
		c := func(i int) bool {
			//ee,_:=strconv.Atoi(elements[i].key())
			//kk,_:=strconv.Atoi(key)
			return elements[i].key() <= key
		}
		idx := sort.Search(int(p.count), c)
		if idx < int(p.count) {
			*stack = append(*stack, pagestack{pageid: p.curid, index: idx})
			sub := bt.getpage(uint32(elements[idx].value))
			return sub.search(key, stack, bt)
		}

		//没有找到,需要添加
		*stack = append(*stack, pagestack{pageid: p.curid, index: -1})
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
	db       *BTreedb
	name     string
	root     *page
	rootpgid uint32
	//cache map[uint32]*page
}

func loadbtree(name string, root *page, db *BTreedb) *btree {

	bt := &btree{db: db, name: name, root: root, rootpgid: root.curid}
	return bt

}

func newbtree(name string, db *BTreedb) *btree {
	bt := &btree{db: db, name: name}
	bt.root, _, _ = bt.newpage(0, 0, tinterior)
	var leaf *page
	leaf, bt.root, _ = bt.newpage(bt.root.curid, 0, tleaf)
	ele := bt.root.getElement(0)
	ele.value = uint64(leaf.curid)
	bt.rootpgid = bt.root.curid
	return bt
}

func (bt *btree) Set(key string, value uint64) error {
	bt.root = bt.db.getpage(bt.rootpgid)
	res := bt.root.set(key, value, bt)
	if res {
		//bt.db.Sync()
		return nil
	}

	return errors.New("update fail")
}

func (bt *btree) checkmmap() error {
	return bt.db.checkmmap()
}

func (bt *btree) newpage(parentid, preid uint32, pagetype uint8) (*page, *page, *page) {

	return bt.db.newpage(parentid, preid, pagetype)
}

func (bt *btree) getpage(pgid uint32) *page {

	// if _,ok:=bt.cache[pgid];ok{
	//      return bt.cache[pgid]
	//  }
	//pg:= bt.db.getpage(pgid)
	//  bt.cache[pgid]=pg
	//  return pg
	return bt.db.getpage(pgid)
}

func (bt *btree) Search(key string) (bool, uint64) {
	bt.root = bt.db.getpage(bt.rootpgid)
	stack := make([]pagestack, 0)
	ok, value, _, _ := bt.root.search(key, &stack, bt)

	return ok, value
}

func (bt *btree) Range(start, end string) (bool, []uint64) {

	if len(start) == 0 {
		bt.root = bt.db.getpage(bt.rootpgid)
		stack1 := make([]pagestack, 0)
		ok, _, _, _ := bt.root.search(end, &stack1, bt)
		if !ok {
			return false, nil
		}
		startpgid := stack1[len(stack1)-1].pageid
		startpg := bt.db.getpage(startpgid)
		res := make([]uint64, 0)
		for idx := stack1[len(stack1)-1].index - 1; idx >= 0; idx-- {
			res = append(res, startpg.getElement(idx).value)
		}

		pgid := startpg.preid
		for pgid != 0 {
			pg := bt.db.getpage(pgid)
			for idx := int(pg.count) - 1; idx > 0; idx-- {
				res = append(res, pg.getElement(idx).value)
			}
			pgid = pg.preid
		}
		return true, res
	}

	if len(end) == 0 {
		bt.root = bt.db.getpage(bt.rootpgid)
		stack1 := make([]pagestack, 0)
		ok, _, _, _ := bt.root.search(start, &stack1, bt)
		if !ok {
			return false, nil
		}
		startpgid := stack1[len(stack1)-1].pageid
		startpg := bt.db.getpage(startpgid)
		res := make([]uint64, 0)
		for idx := stack1[len(stack1)-1].index; idx < int(startpg.count); idx++ {
			res = append(res, startpg.getElement(idx).value)
		}

		pgid := startpg.nextid
		for pgid != 0 {
			pg := bt.db.getpage(pgid)
			for idx := 0; idx < int(pg.count); idx++ {
				res = append(res, pg.getElement(idx).value)
			}
			pgid = pg.nextid
		}
		return true, res
	}

	bt.root = bt.db.getpage(bt.rootpgid)
	stack1 := make([]pagestack, 0)
	ok, _, _, _ := bt.root.search(start, &stack1, bt)
	if !ok {
		return false, nil
	}
	startpgid := stack1[len(stack1)-1].pageid

	stack2 := make([]pagestack, 0)
	ok, _, _, _ = bt.root.search(end, &stack2, bt)
	if !ok {
		return false, nil
	}
	endpgid := stack2[len(stack2)-1].pageid

	res := make([]uint64, 0)
	endpg := bt.db.getpage(endpgid)
	for idx := stack2[len(stack2)-1].index; idx < int(endpg.count); idx++ {
		res = append(res, endpg.getElement(idx).value)
	}

	pgid := endpg.nextid
	for pgid != startpgid && pgid != 0 {
		pg := bt.db.getpage(pgid)
		for idx := 0; idx < int(pg.count); idx++ {
			res = append(res, pg.getElement(idx).value)
		}
		pgid = pg.nextid
	}

	startpg := bt.db.getpage(startpgid)

	for idx := 0; idx < stack1[len(stack1)-1].index; idx++ {
		res = append(res, startpg.getElement(idx).value)
	}

	return true, res

}

func (bt *btree) Display() {
	bt.root.display(bt)
}

type metaBT struct {
	btname    [32]byte
	btnamelen uint32
	maxkeylen uint32
	rootpgid  uint32
}

func (mt *metaBT) key() string {
	return string(mt.btname[:mt.btnamelen])
}

func (mt *metaBT) setkey(key string) {
	if len(key) == 0 {
		return
	}
	if len(key) > 32 {
		copy(mt.btname[:], []byte(key)[:32])
		mt.btnamelen = 32
		return
	}
	copy(mt.btname[:len(key)], []byte(key)[:])
	mt.btnamelen = uint32(len(key))
	return
}

type metaInfo struct {
	magic   uint32
	maxpgid uint32
	btnum   uint32
	btinfos [64]metaBT
}

func (mi *metaInfo) addbt(name string, rootpgid uint32) error {

	mi.btinfos[mi.btnum].setkey(name)
	mi.btinfos[mi.btnum].rootpgid = rootpgid
	mi.btnum++
	return nil

}

const magicnum uint32 = 0x9EDFEDFA

type BTreedb struct {
	btmap     map[string]*btree // btree集合
	filename  string
	mmapbytes []byte
	//maxpgid   uint32
	fd   *os.File
	meta *metaInfo
}

func exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func NewBTDB(dbname string) *BTreedb {

	fmt.Printf("headoffset : %v \n", pageheadOffset)
	fmt.Printf("elementSize: %v \n", elementSize)
	fmt.Printf("pageheaadlen: %v \n", pageheaadlen)
	fmt.Printf("btdbname : %v \n", dbname)
	file_create_mode := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	this := &BTreedb{filename: dbname, btmap: make(map[string]*btree)}

	if exist(dbname) {
		file_create_mode = os.O_RDWR
	} else {
		file_create_mode = os.O_RDWR | os.O_CREATE | os.O_TRUNC
	}

	f, err := os.OpenFile(dbname, file_create_mode, 0664)
	if err != nil {
		return nil
	}

	fi, _ := f.Stat()
	filelen := fi.Size()
	fmt.Printf("filelen : %v, %v \n", filelen, pagesize*2)
	if filelen < pagesize*2 {
		syscall.Ftruncate(int(f.Fd()), pagesize*2)
		filelen = pagesize * 2
		this.fd = f
		//var addr = unsafe.Pointer(&this.mmapbytes[0])
		this.mmapbytes, err = syscall.Mmap(int(f.Fd()), 0, int(filelen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		//ptr, err := C.mmap(addr, C.size_t(filelen), C.PROT_READ|C.PROT_WRITE, C.MAP_SHARED, C.int(f.Fd()), 0)

		if err != nil {
			fmt.Printf("MAPPING ERROR  %v \n", err)
			return nil
		}
		//this.mmapbytes = ([]byte)(unsafe.Pointer(ptr))
		this.meta = (*metaInfo)(unsafe.Pointer(&this.mmapbytes[0]))
		this.meta.magic = magicnum
		this.meta.maxpgid = 1
		this.meta.btnum = 0
		return this
	}
	this.fd = f
	this.mmapbytes, err = syscall.Mmap(int(f.Fd()), 0, int(filelen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return nil
	}
	this.meta = (*metaInfo)(unsafe.Pointer(&this.mmapbytes[0]))
	if this.meta.magic != magicnum {
		fmt.Printf("FILE TYPE ERROR \n")
		return nil
	}

	for i := uint32(0); i < this.meta.btnum; i++ {
		btname := this.meta.btinfos[i].key()
		root := this.getpage(this.meta.btinfos[i].rootpgid)
		this.btmap[btname] = loadbtree(btname, root, this)
	}

	return this
}

func (db *BTreedb) AddBTree(name string) error {

	if _, ok := db.btmap[name]; ok {
		fmt.Printf("ERROR:::%v\n", db.filename)
		return nil
	}
	//fmt.Printf("FILE:::%v\n", db.filename)
	bt := newbtree(name, db)
	if bt == nil {
		fmt.Printf("create error:::%v\n", name)
		return errors.New("create error")
	}
	db.btmap[name] = bt
	db.meta.addbt(name, bt.root.curid)
	db.Sync()
	return nil
}

func (db *BTreedb) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(&db.mmapbytes))
}

func (db *BTreedb) Sync() error {
	dh := db.header()
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, dh.Data, uintptr(dh.Len), syscall.MS_SYNC)
	if err != 0 {
		fmt.Printf("Sync Error ")
		return errors.New("Sync Error")
	}
	return nil
}

func (db *BTreedb) Set(btname, key string, value uint64) error {

	if _, ok := db.btmap[btname]; !ok {
		return errors.New("has one")
	}

	return db.btmap[btname].Set(key, value)

}

func (db *BTreedb) Search(btname, key string) (bool, uint64) {
	if _, ok := db.btmap[btname]; !ok {
		return false, 0
	}

	return db.btmap[btname].Search(key)

}

func (db *BTreedb) Range(btname, start, end string) (bool, []uint64) {

	if _, ok := db.btmap[btname]; !ok {
		return false, nil
	}

	if start >= end && len(end) > 0 && len(start) > 0 {
		fmt.Printf("START OVER END\n")
		return false, nil
	}

	return db.btmap[btname].Range(start, end)

}

func (db *BTreedb) Close() error {

	syscall.Munmap(db.mmapbytes)
	db.fd.Close()
	return nil
}

func (bt *BTreedb) newpage( /*parent, pre *page*/ parentid, preid uint32, pagetype uint8) (*page, *page, *page) {

	if bt.checkmmap() != nil {
		fmt.Printf("check error \n")
		return nil, nil, nil
	}
	var parent *page
	var pre *page
	lpage := (*page)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.meta.maxpgid) * pagesize)]))
	//fmt.Printf("lapge:%v\n", unsafe.Pointer(lpage))
	lpage.curid = bt.meta.maxpgid
	lpage.pgtype = pagetype
	lpage.nextid = 0
	lpage.preid = 0
	if pagetype == tinterior {
		lpage.count = 1
		ele := (*[0xFFFF]element)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.meta.maxpgid)*pagesize + pageheadOffset)]))
		lpage.used = uint32(pageheaadlen)
		ele[0].setkv("", 0)
		lpage.elementsptr = uintptr(unsafe.Pointer(ele))

	} else {
		lpage.count = 0
		ele := (*[0xFFFF]element)(unsafe.Pointer(&bt.mmapbytes[(int64(bt.meta.maxpgid)*pagesize + pageheadOffset)]))
		lpage.elementsptr = uintptr(unsafe.Pointer(ele))
		lpage.used = uint32(pageheaadlen)
	}
	//fmt.Printf("lapge:%v\n", unsafe.Pointer(lpage))
	//fmt.Printf("parent:%v\n", unsafe.Pointer(parent))
	if parentid != 0 {
		parent = bt.getpage(parentid)
		lpage.parentpg = parent.curid
	} else {
		lpage.parentpg = 0
	}

	if preid != 0 {
		pre = bt.getpage(preid)
		lpage.nextid = pre.nextid
		pre.nextid = lpage.curid
		lpage.preid = pre.curid
	}

	bt.meta.maxpgid++
	return lpage, parent, pre
}

func (bt *BTreedb) checkmmap() error {
	if int(int64(bt.meta.maxpgid)*pagesize) >= len(bt.mmapbytes) {
		err := syscall.Ftruncate(int(bt.fd.Fd()), int64(bt.meta.maxpgid+1)*pagesize)
		if err != nil {
			fmt.Printf("ftruncate error : %v\n", err)
			return err
		}
		maxpgid := bt.meta.maxpgid
		syscall.Munmap(bt.mmapbytes)
		//fmt.Printf(".meta.maxpgid:%v\n",bt.meta.maxpgid)
		bt.mmapbytes, err = syscall.Mmap(int(bt.fd.Fd()), 0, int(int64( /*bt.meta.maxpgid*/ maxpgid+1)*pagesize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

		if err != nil {
			fmt.Printf("MAPPING ERROR  %v \n", err)
			return err
		}

		bt.meta = (*metaInfo)(unsafe.Pointer(&bt.mmapbytes[0]))

	}
	return nil
}

func (bt *BTreedb) getpage(pgid uint32) *page {
	//fmt.Printf("pgid:%v\n",pgid)
	return (*page)(unsafe.Pointer(&bt.mmapbytes[(int64(pgid) * pagesize)]))

}

func (db *BTreedb) setrootid(btname string) error {

	if _, ok := db.btmap[btname]; !ok {
		return errors.New("no bt")
	}

	for i := uint32(0); i < db.meta.btnum; i++ {
		if db.meta.btinfos[i].key() == btname {
			db.meta.btinfos[i].rootpgid = db.btmap[btname].root.curid
		}

	}

	return nil
}
