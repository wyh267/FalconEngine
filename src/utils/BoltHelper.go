/*****************************************************************************
 *  file name : BoltHelper.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : bolt封装类，可选单体模式
 *
******************************************************************************/
package utils

import (
	"bolt"
	"encoding/json"
	"fmt"
)

type BoltHelper struct {
	name   string
	db     *bolt.DB
	Logger *Log4FE
}

var gBoltHelper *BoltHelper = nil

// GetBoltInstance function description : 获取单例引用
// params :
// return :
func GetBoltInstance() *BoltHelper {

	if gBoltHelper == nil {
		var err error
		this := &BoltHelper{name: "default.db", Logger: nil}
		this.db, err = bolt.Open(this.name, 0666, nil)
		if err != nil {
			this.Logger.Error("[ERROR] Open Dbname Error %v", err)
		}
		gBoltHelper = this
	}
	return gBoltHelper
}

// NewBoltInstance function description : 新建单体引用
// params :
// return :
func NewBoltInstance(dbname string, mode int, logger *Log4FE) *BoltHelper {

	if gBoltHelper != nil {
		return gBoltHelper
	}
	var err error
	gBoltHelper = &BoltHelper{name: dbname, Logger: logger}
	gBoltHelper.db, err = bolt.Open(dbname, 0666, nil)
	if err != nil {
		gBoltHelper.Logger.Error("[ERROR] Open Dbname Error %v", err)
	}

	return gBoltHelper

}

// NewBoltHelper function description : 新建Bolt
// params :
// return :
func NewBoltHelper(dbname string, mode int, logger *Log4FE) *BoltHelper {
	var err error
	this := &BoltHelper{name: dbname, Logger: logger}
	this.db, err = bolt.Open(dbname, 0666, nil)
	if err != nil {
		this.Logger.Error("[ERROR] Open Dbname Error %v", err)
	}

	return this

}

// CreateTable function description : 新建表
// params :
// return :
func (this *BoltHelper) CreateTable(tablename string) (*bolt.Bucket, error) {

	tx, err := this.db.Begin(true)
	if err != nil {
		this.Logger.Error("[ERROR] Create Tx Error %v ", err)
		return nil, err
	}
	defer tx.Rollback()
	//func (*Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error)
	table, err := tx.CreateBucketIfNotExists([]byte(tablename))
	if err != nil {
		this.Logger.Error("[ERROR] Create Table Error %v", err)
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		this.Logger.Error("[ERROR] Commit Tx Error %v", err)
		return nil, err
	}

	return table, nil
}

func (this *BoltHelper) DeleteTable(tablename string) error {

	tx, err := this.db.Begin(true)
	if err != nil {
		this.Logger.Error("[ERROR] DeleteTable Tx Error %v ", err)
		return err
	}
	defer tx.Rollback()
	//func (*Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error)
	err = tx.DeleteBucket([]byte(tablename))
	if err != nil {
		this.Logger.Warn("[WARN] DeleteTable Table Error %v", err)
	}

	if err := tx.Commit(); err != nil {
		this.Logger.Error("[ERROR] Commit Tx Error %v", err)
		return err
	}

	return nil

}

// Update function description : 更新数据
// params :
// return :
func (this *BoltHelper) Update(tablename, key, value string) error {

	err := this.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tablename))
		if b == nil {
			this.Logger.Error("[ERROR] Tablename[%v] not found", tablename)
			return fmt.Errorf("Tablename[%v] not found", tablename)
		}
		err := b.Put([]byte(key), []byte(value))
		return err
	})

	return err
}







func (this *BoltHelper) UpdateObj(tablename, key string, obj interface{}) error {

	value, err := json.Marshal(obj)
	if err != nil {
		this.Logger.Error("%v", err)
		return err
	}

	err = this.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tablename))
		if b == nil {
			this.Logger.Error("[ERROR] Tablename[%v] not found", tablename)
			return fmt.Errorf("Tablename[%v] not found", tablename)
		}
		err := b.Put([]byte(key), value)
		return err
	})

	return err
}

func (this *BoltHelper) HasKey(tablename, key string) bool {

	if _, err := this.Get(tablename, key); err != nil {
		return false
	}

	return true

}

func (this *BoltHelper) Get(tablename, key string) (string, error) {

	var value []byte

	this.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(tablename))
		value = b.Get([]byte(key))
		//fmt.Printf("value : %v\n", string(value))
		return nil
	})

	if value == nil {
		//this.Logger.Error("[ERROR] Key %v not found",key)
		return "", fmt.Errorf("Key[%v] Not Found", key)
	}

	return string(value), nil

}


func (this *BoltHelper) GetValue(tablename, key string) ([]byte, error) {

	var value []byte

	this.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(tablename))
		value = b.Get([]byte(key))
		//fmt.Printf("value : %v\n", string(value))
		return nil
	})

	if value == nil {
		//this.Logger.Error("[ERROR] Key %v not found",key)
		return nil, fmt.Errorf("Key[%v] Not Found", key)
	}

	return value, nil

}



func (this *BoltHelper) CloseDB() error {
	
	return this.db.Close()
}




func (this *BoltHelper) DisplayTable(tablename string) error {

	this.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(tablename))

		b.ForEach(func(k, v []byte) error {
			fmt.Printf("key=%s, value=%s\n", k, v)
			return nil
		})
		return nil
	})

	return nil

}

func (this *BoltHelper) Traverse(tablename string,tx *bolt.Tx) func() ([]byte, []byte) {

	var c *bolt.Cursor
	
	b:=tx.Bucket([]byte(tablename))
	c = b.Cursor()
	

	k, v := c.First()
	return func() ([]byte, []byte) {

		if k != nil {
			k1, v1 := k, v
			k, v = c.Next()
			return k1, v1
		}

		return nil, nil

	}

}




func (this *BoltHelper)BeginTx() (*bolt.Tx,error) {
	

	tx, err := this.db.Begin(true)
	if err != nil {
		this.Logger.Error("[ERROR] Create Tx Error %v ", err)
		return nil,err
	}
	return tx,nil

	
}


func (this *BoltHelper)Commit(tx *bolt.Tx) error {
	
	if err := tx.Commit(); err != nil {
		this.Logger.Error("[ERROR] Commit Tx Error %v", err)
		tx.Rollback()
		return err
	}

	return nil
}