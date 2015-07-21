package BaseFunctions

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/outmana/log4jzl"
	"time"
)

type RedisClient struct {
	conn   redis.Conn
	pool   *redis.Pool
	config *Configure
	logger *log4jzl.Log4jzl
}

func NewRedisClient(config *Configure, logger *log4jzl.Log4jzl) (*RedisClient, error) {
	counter := &RedisClient{}
	counter.config = config
	counter.logger = logger

	counter.pool = &redis.Pool{
		MaxIdle:     30,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			host, _ := config.GetRedisHost()
			port, _ := config.GetRedisPort()

			connStr := fmt.Sprintf("%v:%v", host, port)
			c, err := redis.Dial("tcp", connStr)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return counter, nil
}

func (this *RedisClient) Release() {
	this.pool.Close()
}

func (this *RedisClient) SetFields(doc_id int64, fields map[string]string) error {
	key := fmt.Sprintf("DOC_ID:%v", doc_id)
	//var value string
	var comm []interface{}
	comm = append(comm, key)
	for k, v := range fields {
		//v:=fmt.Sprintf(" %v \"%v\"",k,v)
		//value = value + v
		comm = append(comm, k)
		comm = append(comm, v)
	}
	//comm := fmt.Sprintf("%v%v",key,value)
	fmt.Printf("REDIS :: %v\n", comm)
	conn := this.pool.Get()
	_, err := conn.Do("HMSET", comm...)
	if err != nil {
		this.logger.Error("REDIS ERROR : %v ", err)
		return err
	}

	return nil

}
