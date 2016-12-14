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
	"fmt"
	"os"
	"reflect"
	"strconv"
	//"strconv"
	"bolt"
	"unsafe"
	"utils"
)

const magicnum uint32 = 0x9EDFEDFA

type BTreedb struct {
	//btmap     map[string]*btree // btree集合
	filename  string
	mmapbytes []byte
	//maxpgid   uint32
	fd *os.File
	//meta *metaInfo

	dbHelper *utils.BoltHelper
	buckets  map[string]*bolt.Tx
	logger   *utils.Log4FE
}

func exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func NewBTDB(dbname string, logger *utils.Log4FE) *BTreedb {

	this := &BTreedb{filename: dbname, dbHelper: nil, logger: logger, buckets: make(map[string]*bolt.Tx)}
	this.dbHelper = utils.NewBoltHelper(dbname, 0, logger)

	return this

}

func (db *BTreedb) AddBTree(name string) error {

	_, err := db.dbHelper.CreateTable(name)
	return err

}

func (db *BTreedb) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(&db.mmapbytes))
}

func (db *BTreedb) Sync() error {

	return nil
}

func (db *BTreedb) Set(btname, key string, value uint64) error {

	return db.dbHelper.Update(btname, key, fmt.Sprintf("%v", value))

}

func (db *BTreedb) MutiSet(btname string, kv map[string]string) error {
	return db.dbHelper.SetBatch(btname, kv)
	//return db.dbHelper.UpdateMuti(btname, kv)

}

func (db *BTreedb) SetBatch(btname string, btMap map[string]uint64) error {

	return nil //db.dbHelper.SetBatch(btname, btMap)

}

func (db *BTreedb) IncValue(btname, key string) error {

	found, value := db.Search(btname, key)
	if found {

		value++

	} else {
		value = 1

	}

	return db.dbHelper.Update(btname, key, fmt.Sprintf("%v", value))

}

func (db *BTreedb) Search(btname, key string) (bool, uint64) {

	//db.logger.Info("Search btname : %v  key : %v  ",btname,key)
	vstr, err := db.dbHelper.Get(btname, key)
	if err != nil {
		return false, 0
	}
	//db.logger.Info("Search btname : %v  key : %v value str : %v ",btname,key,vstr)
	u, e := strconv.ParseUint(vstr, 10, 64)
	if e != nil {
		return false, 0
	}
	//db.logger.Info("Search btname : %v  key : %v value  : %v ",btname,key,u)
	return true, u

}

func (db *BTreedb) GetFristKV(btname string) (string, uint32, uint32, int, bool) {
	//db.logger.Info("Search btname : %v  key : %v  ",btname,key)
	key, vstr, err := db.dbHelper.GetFristKV(btname)
	if err != nil {
		return "", 0, 0, 0, false
	}
	//db.logger.Info("Search btname : %v  key : %v value str : %v ",btname,key,vstr)
	u, e := strconv.ParseUint(vstr, 10, 64)
	if e != nil {
		return "", 0, 0, 0, false
	}
	//db.logger.Info("Search btname : %v  key : %v value  : %v ",btname,key,u)
	return key, uint32(u), 0, 0, true

}

func (db *BTreedb) GetNextKV(btname, key string /*pagenum uint32, index int*/) (string, uint32, uint32, int, bool) {

	vkey, vstr, err := db.dbHelper.GetNextKV(btname, key)
	if err != nil {
		return "", 0, 0, 0, false
	}
	//db.logger.Info("Search btname : %v  key : %v value str : %v ",btname,key,vstr)
	u, e := strconv.ParseUint(vstr, 10, 64)
	if e != nil {
		return "", 0, 0, 0, false
	}
	//db.logger.Info("Search btname : %v  key : %v value  : %v ",btname,key,u)
	return vkey, uint32(u), 0, 0, true

}

func (db *BTreedb) Close() error {

	return db.dbHelper.CloseDB()

}
