//	路由分发器
//
package BaseFunctions

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"github.com/outmana/log4jzl"
	"io/ioutil"
	"math/rand"
	"time"
	"utils"

)

type Router struct {
	Configure  *Configure
	Logger        *log4jzl.Log4jzl
	Processors	  map[string]FEProcessor
}

//路由设置
//数据分发
func (this *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	
	var err error
	var body []byte
	var startTime, endTime time.Time

	startTime = time.Now()
	functime := utils.InitTime()
	result := make(map[string]interface{})
	
	header := w.Header()
	header.Add("Content-Type", "application/json")
	header.Add("charset", "UTF-8")
	
	//生成log_id
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	log_id1 := rand.Intn(100000)
	log_id2 := rand.Intn(100000)
	log_id := fmt.Sprintf("%d-%d", log_id1, log_id2)
	

	stype,err := this.ParseURL(r.RequestURI)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, MakeErrorResult(-1, err.Error()))
	} else {
		RequestParams, err := this.parseArgs(r)
		if err != nil {
			result["error_code"] = -1
			result["message"] = "解析参数错误" //err.Error()
			goto END
		}

		body, err = ioutil.ReadAll(r.Body)
		if err != nil && err != io.EOF {
			result["error_code"] = -1
			result["message"] = "读取请求数据出错，请重新提交" //err.Error()
			goto END
		}
		//处理业务逻辑
		if stype == 1 { //搜索
			err := this.Processors["search"].Process(log_id,body,RequestParams,result,functime)
			if err !=nil{
				goto END
			}
		}
		
		if stype == 2 { //数据更新
			err := this.Processors["update"].Process(log_id,body,RequestParams,result,functime)
			if err !=nil{
				goto END
			}
		}
		
		if stype == 3 { //监控，控制
			
		}
		
	}

END:
	if err != nil {
		this.Logger.Error("[LOG_ID:%v] %v", log_id, err)
	}

	endTime = time.Now()
	result["cost"] = fmt.Sprintf("%v", endTime.Sub(startTime))
	result["request_url"] = r.RequestURI
	this.Logger.Info("[LOG_ID:%v] [COST:%v]", log_id, result["cost"])
	resStr, _ := this.createJSON(result)
	io.WriteString(w, resStr)
	return 
}



func (this *Router) createJSON(result map[string]interface{}) (string, error) {
	r, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(r), nil
}


//
//通过正则表达式选择路由程序
//
func (this *Router) ParseURL(url string) (int64,error) {
	//确定是否是本服务能提供的控制类型

	urlPattern:= "(search|update|contrl)\\?"//this.Configure.GetUrlPattern()
	urlRegexp, err := regexp.Compile(urlPattern)
	if err != nil {
		return -1,err
	}
	matchs := urlRegexp.FindStringSubmatch(url)
	if matchs == nil {
		return -1,errors.New("URL ERROR ")
	}
	resource := matchs[1]
	if resource == "search"{
		return 1,nil
	}
	if resource == "update"{
		return 2,nil
	}
	if resource == "update"{
		return 3,nil
	}
	
	return -1,errors.New("Error")
	
/*	
	
	urlPattern= "update\\?"//this.Configure.GetUrlPattern()
	urlRegexp, err = regexp.Compile(urlPattern)
	if err != nil {
		return -1,err
	}
	matchs = urlRegexp.FindStringSubmatch(url)
	if matchs != nil {
		return 2,nil
	}
	
	
	return -1,errors.New("err")
	
	*/
}

func MakeErrorResult(errcode int, errmsg string) string {
	data := map[string]interface{}{
		"error_code": errcode,
		"message":    errmsg,
	}
	result, err := json.Marshal(data)
	if err != nil {
		return fmt.Sprintf("{\"error_code\":%v,\"message\":\"%v\"}", errcode, errmsg)
	}
	return string(result)
}




func (this *Router) parseArgs(r *http.Request) (map[string]string, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, err
	}

	//每次都重新生成一个新的map，否则之前请求的参数会保留其中
	res := make(map[string]string)
	fmt.Printf("Form :: %v ",r.Form)
	for k, v := range r.Form {
		res[k] = v[0]
	}

	return res, nil
}