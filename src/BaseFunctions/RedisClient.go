package BaseFunctions

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/outmana/log4jzl"
	"time"
	//"errors"
	"strings"
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
		MaxIdle:     300,
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


func (this *RedisClient) GetFields(PK interface{},fields []string) ( map[string]string ,error) {
	key := fmt.Sprintf("PK:%v",PK)
	//var value string
	var comm []interface{}
	comm = append(comm, key)
	for _, v := range fields {
		comm = append(comm, v)
	}
	//fmt.Printf("Comm : %v \n",comm)
	
	conn := this.pool.Get()
	defer conn.Close()
	reply, err := redis.MultiBulk(conn.Do("HMGET", comm...))
    if err != nil {
        return nil, err
    }
    //var list = make([]string, 0)
    var res = make(map[string]string)
	for index, v := range reply {
        s, err := redis.String(v, nil)
        if err != nil {
            return nil,err
        }
        s = strings.Trim(s, "\"")
		res[fields[index]]=s
       // list = append(list, s)
    }
	//fmt.Printf("\n ALL REDIS RESULT :: %v\n", res)
    return res, nil
	
	
	/*
	res, err := redis.Values(conn.Do("HMGET", comm...))
	if err != nil {
		this.logger.Error("REDIS ERROR : %v ", err)
		return nil,err
	}
	
	fmt.Printf("\n ALL REDIS RESULT :: %v\n", res)
	values :=make([]string,len(fields))
	if _, err := redis.Scan(res, values...); err != nil {
    	// handle error
		return nil,err
	}
	fmt.Printf("REDISE STRING :: ")
	for _,v:=range values{
		fmt.Printf(" %v ",v)
	}
	fmt.Printf("\n")
	*/
	
	return nil, err
}



func (this *RedisClient) SetFields(doc_id int64, fields map[string]string) error {
	key := fmt.Sprintf("PK:%v", fields["id"])
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
	//fmt.Printf("REDIS :: %v\n", comm)
	conn := this.pool.Get()
	defer conn.Close()
	_, err := conn.Do("HMSET", comm...)
	if err != nil {
		this.logger.Error("REDIS ERROR : %v ", err)
		return err
	}

	return nil

}
