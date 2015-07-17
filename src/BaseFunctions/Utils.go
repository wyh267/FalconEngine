//工具包
package BaseFunctions

import (
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
)

func SerializeObject(obj interface{}) (string, error) {
	r, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}

	result := string(r)
	if result == "null" {
		return "", errors.New("null object")
	}

	return result, nil
}

func DeserializeObject(str string) (interface{}, error) {
	var obj interface{}

	err := json.Unmarshal([]byte(str), &obj)
	if err != nil {
		return nil, err
	}

	if obj == nil {
		return nil, errors.New("null object")
	}

	return obj, nil
}

func TransNumToString(num int64) (string, error) {

	var base int64
	base = 62
	baseHex := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	output_list := list.New()
	for num/base != 0 {
		output_list.PushFront(num % base)
		num = num / base
	}
	output_list.PushFront(num % base)
	str := ""
	for iter := output_list.Front(); iter != nil; iter = iter.Next() {
		str = str + string(baseHex[int(iter.Value.(int64))])
	}

	return str, nil
}

func RequestUrl(url string) ([]byte, error) {

	client := &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				conn, err := net.DialTimeout(netw, addr, time.Second*2)
				if err != nil {
					return nil, err
				}
				conn.SetDeadline(time.Now().Add(time.Second * 2))
				return conn, nil
			},
			ResponseHeaderTimeout: time.Second * 2,
		},
	}
	rsp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()
	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil

}

type TaskDelInfo struct {
	Task_id int64  `json:"task_id"`
	Cid     int64  `json:"cid"`
	Status  string `json:"status"`
}

type TaskScheduleInfo struct {
	ErrorCode int64  `json:"error_code"`
	ErrorMsg  string `json:"error_msg"`
}

func PostRequest(url string, b []byte) ([]byte, error) {

	client := &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				conn, err := net.DialTimeout(netw, addr, time.Second*2)
				if err != nil {
					return nil, err
				}
				conn.SetDeadline(time.Now().Add(time.Second * 2))
				return conn, nil
			},
			ResponseHeaderTimeout: time.Second * 2,
		},
	}

	body := bytes.NewBuffer([]byte(b))
	res, err := client.Post(url, "application/json;charset=utf-8", body)
	if err != nil {

		return nil, err
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {

		return nil, err
	}

	return result, nil

}

func DeleteTask(task_id, cid int64, host, port string) error {

	var s TaskDelInfo
	s.Task_id = task_id
	s.Cid = cid
	s.Status = "DEL"
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}

	client := &http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				conn, err := net.DialTimeout(netw, addr, time.Second*2)
				if err != nil {
					return nil, err
				}
				conn.SetDeadline(time.Now().Add(time.Second * 2))
				return conn, nil
			},
			ResponseHeaderTimeout: time.Second * 2,
		},
	}

	body := bytes.NewBuffer([]byte(b))
	url := fmt.Sprintf("http://%v:%v/collectionjob", host, port)
	//fmt.Printf("URL:::%v",url)
	res, err := client.Post(url, "application/json;charset=utf-8", body)
	if err != nil {

		return err
	}
	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {

		return err
	}
	//fmt.Printf("RESULT:::::%v\n", string(result))
	var taskScheduleInfo TaskScheduleInfo
	err = json.Unmarshal(result, &taskScheduleInfo)
	if err != nil {
		return err
	}
	if taskScheduleInfo.ErrorCode != 0 {
		return errors.New(taskScheduleInfo.ErrorMsg)
	}
	return nil

}

func CheckRegexp(base_str, urlPattern string) bool {
	urlRegexp, err := regexp.Compile(urlPattern)
	if err != nil {
		return false
	}
	matchs := urlRegexp.FindStringSubmatch(base_str)
	if matchs == nil {
		return false
	}
	return true
}

func CalcDateRangeForCTA(date string) (string, string) {

	date_range := CalcDateRange(date)
	d := strings.Split(date_range, ",")
	if len(d) == 2 {
		return d[0] + " 00:00:00", d[1] + " 23:59:59"
	}
	return "2015-01-01 00:00:00", "2015-01-02 23:59:59"

}

func CalcDateRangeSplit(date string) (string, string) {
	date_range := CalcDateRange(date)
	d := strings.Split(date_range, ",")
	if len(d) == 2 {
		return d[0], d[1]
	}
	return "2015-01-01", "2015-01-02"
}

func CalcDateRange(date string) string {

	var date_range string
	switch date {
	case "day":
		start := time.Now()
		start = start.AddDate(0, 0, -1)
		date_range = fmt.Sprintf("%v,%v", start.Format("2006-01-02"), time.Now().Format("2006-01-02"))
	case "week":
		end := time.Now()
		end_weekday := end.Weekday()
		start := end.AddDate(0, 0, 0-int(end_weekday))
		end = end.AddDate(0, 0, 1)
		date_range = fmt.Sprintf("%v,%v", start.Format("2006-01-02"), time.Now().Format("2006-01-02"))
		//fmt.Printf("%v\n",date_range)
	case "month":
		end := time.Now()
		day := end.Day()
		start := end.AddDate(0, 0, 1-day)
		date_range = fmt.Sprintf("%v,%v", start.Format("2006-01-02"), time.Now().Format("2006-01-02"))
		//fmt.Printf("%v\n",date_range)
	case "lastmonth":
		end := time.Now()
		day := end.Day()
		start := end.AddDate(0, -1, 0-day+1)
		end = end.AddDate(0, 0, 0-day+1)
		date_range = fmt.Sprintf("%v,%v", start.Format("2006-01-02"), end.Format("2006-01-02"))
		//fmt.Printf("%v\n",date_range)
	case "last30day":
		end := time.Now()
		start := end.AddDate(0, 0, -30)
		date_range = fmt.Sprintf("%v,%v", start.Format("2006-01-02"), time.Now().Format("2006-01-02"))
		//fmt.Printf("%v\n",date_range)
	case "last3month":
		end := time.Now()
		end = end.AddDate(0, 0, 1)
		start := end.AddDate(0, -3, 0)
		date_range = fmt.Sprintf("%v,%v", start.Format("2006-01-02"), time.Now().Format("2006-01-02"))
		//fmt.Printf("%v\n",date_range)
	default:
		end := time.Now()
		day := end.Day()
		end = end.AddDate(0, 0, 1)
		start := end.AddDate(0, 0, 0-day)
		date_range = fmt.Sprintf("%v,%v", start.Format("2006-01-02"), time.Now().Format("2006-01-02"))
		//fmt.Printf("%v\n",date_range)
	}

	return date_range

}

func FormatDateTime(datetime string) (string, error) {
	t, err := time.Parse("Mon Jan 2 15:04:05 -0700 2006", datetime)
	if err != nil {
		return "", err
	}
	return t.Format("2006-01-02 15:04:05"), nil
}

func FormatWeiboText(text string) string {
	urlRegExp := regexp.MustCompile(`((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,5})?(/[a-zA-Z0-9_\.?=&#+-]*)*`)
	formatText := urlRegExp.ReplaceAllStringFunc(text, func(s string) string {
		return fmt.Sprintf("<a href=\"%v\" rel=\"nofollow\" target=\"_blank\">%v</a>", s, s)
	})
	return formatText
}
