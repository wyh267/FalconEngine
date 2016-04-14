package utils

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type MysqlDBAdaptor struct {
	db     *sql.DB
	Logger *Log4FE
}

func NewMysqlDBAdaptor(user,password,host,port,dbname,charset string, logger *Log4FE) (*MysqlDBAdaptor, error) {
	dbAdaptor := &MysqlDBAdaptor{}
	dbAdaptor.Logger = logger

	conn_str := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", user, password, host, port, dbname, charset)

	var err error
	dbAdaptor.db, err = sql.Open("mysql", conn_str)
	if err != nil {
		return nil, err
	}

	maxOpenConns:= 40
	maxIdleConns := 20

	dbAdaptor.db.SetMaxOpenConns(maxOpenConns)
	dbAdaptor.db.SetMaxIdleConns(maxIdleConns)

	err = dbAdaptor.db.Ping()
	if err != nil {
		return nil, err
	}

	return dbAdaptor, nil
}

func (this *MysqlDBAdaptor) Release() {
	if this.db != nil {
		this.db.Close()
		this.db = nil
	}
}


func (this *MysqlDBAdaptor) QueryFormat(query string, args ...interface{}) (*sql.Rows, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	rows, err := this.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}