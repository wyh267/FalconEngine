package BaseFunctions

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"utils"
)

type DBAdaptor struct {
	db     *sql.DB
	config *Configure
	logger *utils.Log4FE
}

func NewDBAdaptor(config *Configure, logger *utils.Log4FE) (*DBAdaptor, error) {
	dbAdaptor := &DBAdaptor{}
	dbAdaptor.config = config
	dbAdaptor.logger = logger

	user, _ := dbAdaptor.config.GetMysqlUserName()
	password, _ := dbAdaptor.config.GetMysqlPassword()
	host, _ := dbAdaptor.config.GetMysqlHost()
	port, _ := dbAdaptor.config.GetMysqlPort()
	dbname, _ := dbAdaptor.config.GetMysqlDBname()
	charset, _ := dbAdaptor.config.GetMysqlCharset()

	conn_str := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", user, password, host, port, dbname, charset)

	var err error
	dbAdaptor.db, err = sql.Open("mysql", conn_str)
	if err != nil {
		return nil, err
	}

	maxOpenConns, _ := dbAdaptor.config.GetMysqlMaxConns()
	maxIdleConns, _ := dbAdaptor.config.GetMysqlMaxIdleConns()

	dbAdaptor.db.SetMaxOpenConns(maxOpenConns)
	dbAdaptor.db.SetMaxIdleConns(maxIdleConns)

	err = dbAdaptor.db.Ping()
	if err != nil {
		return nil, err
	}

	return dbAdaptor, nil
}

func (this *DBAdaptor) Release() {
	if this.db != nil {
		this.db.Close()
		this.db = nil
	}
}

func (this *DBAdaptor) Query(query string) (*sql.Rows, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	rows, err := this.db.Query(query)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (this *DBAdaptor) QueryRow(query string) (*sql.Row, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	row := this.db.QueryRow(query)

	return row, nil
}

func (this *DBAdaptor) Exec(query string) error {
	if this.db == nil {
		return fmt.Errorf("database object invalid")
	}

	_, err := this.db.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func (this *DBAdaptor) ExecFormat(query string, args ...interface{}) error {
	//fmt.Printf("ExecFormat...\n")
	if this.db == nil {
		return fmt.Errorf("database object invalid")
	}

	_, err := this.db.Exec(query, args...)
	if err != nil {
		//fmt.Printf("NIL...\n")
		return err
	}

	return nil
}

func (this *DBAdaptor) QueryFormat(query string, args ...interface{}) (*sql.Rows, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	rows, err := this.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (this *DBAdaptor) QueryRowFormat(query string, args ...interface{}) (*sql.Row, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	return this.db.QueryRow(query, args...), nil

}

func (this *DBAdaptor) BeginTransaction() (*sql.Tx, error) {
	if this.db == nil {
		return nil, fmt.Errorf("database object invalid")
	}

	tx, err := this.db.Begin()
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (this *DBAdaptor) ExecTransaction(tx *sql.Tx, query string, args ...interface{}) error {
	if this.db == nil {
		return fmt.Errorf("database object invalid")
	}

	smt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	_, err = smt.Exec(args...)
	if err != nil {
		return err
	}

	return nil

}

func (this *DBAdaptor) CommitTransaction(tx *sql.Tx) error {
	if this.db == nil {
		return fmt.Errorf("database object invalid")
	}

	err := tx.Commit()
	if err != nil {
		//fmt.Printf("提交失败\n")
		return err
	}

	return nil
}

func (this *DBAdaptor) RollbackTransaction(tx *sql.Tx) error {

	if this.db == nil {
		return fmt.Errorf("database object invalid")
	}

	err := tx.Rollback()
	if err != nil {
		//fmt.Printf("提交失败\n")
		return err
	}

	return nil

}
