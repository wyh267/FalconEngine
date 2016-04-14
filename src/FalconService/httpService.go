/*****************************************************************************
 *  file name : httpService.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数据层之上的引擎层
 *
******************************************************************************/

package FalconService

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"time"
	"utils"
)

const (
	URL_SEARCH  uint64 = 1 //普通搜索
	URL_UPDATE  uint64 = 2 //更新数据
	URL_CREATE  uint64 = 3 //建立索引
	URL_CONTROL uint64 = 4
	URL_SHOW    uint64 = 5

	URL_DEBUG_SEARCH uint64 = 100 //调试
    URL_LOADDATA uint64 = 101 
	URL_SEARCH_STATUS uint64 = 301 //查看分群起状态
)

const (
	METHOD_GET    string = "GET"
	METHOD_POST   string = "POST"
	METHOD_PUT    string = "PUT"
	METHOD_DELETE string = "DELETE"
)

type HttpService struct {
	Logger *utils.Log4FE `json:"-"`
	engine utils.Engine
}

func NewHttpService(engine utils.Engine, logger *utils.Log4FE) *HttpService {
	this := &HttpService{Logger: logger, engine: engine}
	return this
}

func (this *HttpService) Start() error {

	if this.engine == nil {
		this.Logger.Error("Server start fail: manager is nil")
		return errors.New("Server start fail: manager is nil")
	}

	this.Logger.Info("Server starting")
	addr := fmt.Sprintf(":%d", 9990)
	err := http.ListenAndServe(addr, this)
	if err != nil {
		this.Logger.Error("Server start fail: %v", err)
		return err
	}
	this.Logger.Info("Server started")
	return nil
}

func (this *HttpService) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	var startTime, endTime time.Time
	var err error
    var body []byte
	startTime = time.Now()
	if err != nil {
		this.Logger.Error(" %v", err)
	}
	//写入http头
	header := w.Header()
	header.Add("Content-Type", "application/json")
	header.Add("charset", "UTF-8")
	header.Add("Access-Control-Allow-Origin", "*")
	requestUrl := r.RequestURI
    if requestUrl == "/favicon.ico"{
        return
    }
	result := make(map[string]interface{})
	result["_errorcode"] = 0
	parms, err := this.parseArgs(r)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, MakeErrorResult(-1, err.Error()))
		return
	}

	_, reqType, err := this.ParseURL(requestUrl)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, MakeErrorResult(-1, err.Error()))
		goto END
	}
    
	body, err = ioutil.ReadAll(r.Body)
	if err != nil && err != io.EOF {
		result["_errorcode"] = -1
		result["_errormessage"] = "读取请求数据出错，请重新提交" //err.Error()
		goto END
	}
    
    switch reqType {
        case URL_SEARCH:
            res,err:=this.engine.Search(r.Method, parms, body)
            if err!=nil{
                result["_errorcode"] = -1
                result["_errormessage"] = err.Error()
                goto END
            }
            endTime = time.Now()
	        this.Logger.Info("[COST:%v] [URL : %v] ", fmt.Sprintf("%v", endTime.Sub(startTime)), r.RequestURI)
            //this.Logger.Info("[INFO] res %v",res)
            io.WriteString(w, res)
            return
            
       case URL_CREATE:
            err:=this.engine.CreateIndex(r.Method, parms, body)
            if err!=nil{
                result["_errorcode"] = -1
                result["_errormessage"] = err.Error()
                goto END
            }
            //result["_status"]= "sucess"
            io.WriteString(w, "sucess")
            return
        case URL_UPDATE:
            res,err:=this.engine.UpdateDocument(r.Method,parms,body)
            if err!=nil{
                result["_errorcode"] = -1
                result["_errormessage"] = err.Error()
                goto END
            }
            //result["_status"]= "sucess"
            io.WriteString(w,res)
            return
       case URL_LOADDATA:
            _,err:=this.engine.LoadData(r.Method,parms,body)
            if err!=nil{
                result["_errorcode"] = -1
                result["_errormessage"] = err.Error()
                goto END
            }
            result["_status"]= "sucess"
            //io.WriteString(w,res)
            goto END
            
    }

	//this.engine.Search(utils.FSSearchUnit{})
    
END:
    if err != nil {
        this.Logger.Error("[ERROR] %v ",err)
    }    
	result["_method"] = r.Method
	endTime = time.Now()
	result["_cost"] = fmt.Sprintf("%v", endTime.Sub(startTime))
	result["_request_url"] = r.RequestURI
	resStr, _ := this.createJSON(result)
	io.WriteString(w, resStr)
	this.Logger.Info("[COST:%v] [URL : %v] ", fmt.Sprintf("%v", endTime.Sub(startTime)), r.RequestURI)
	return
}

func (this *HttpService) createJSON(result map[string]interface{}) (string, error) {

	r, err := json.Marshal(result)
	if err != nil {
		return "", err
	}

	return string(r), nil

}

func (this *HttpService) parseArgs(r *http.Request) (map[string]string, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, err
	}

	//每次都重新生成一个新的map，否则之前请求的参数会保留其中
	res := make(map[string]string)
	for k, v := range r.Form {
		res[k] = v[0]
	}

	return res, nil
}

// ParseURL function description : url解析
// params :
// return :
func (this *HttpService) ParseURL(url string) (int, uint64, error) {
	//确定是否是本服务能提供的控制类型

	urlPattern := "/v(\\d)/(_search|_update|_contrl|_create|_show|_debug|_status|_load)\\?"
	urlRegexp, err := regexp.Compile(urlPattern)
	if err != nil {
		return -1, 0, err
	}
	matchs := urlRegexp.FindStringSubmatch(url)
	if matchs == nil {
		return -1, 0, errors.New("URL ERROR ")
	}
	versionNum, _ := strconv.ParseInt(matchs[1], 10, 8)
	version := int(versionNum)

	resource := matchs[2]
	if resource == "_search" {
		return version, URL_SEARCH, nil
	}
	if resource == "_update" {
		return version, URL_UPDATE, nil
	}
	if resource == "_create" {
		return version, URL_CREATE, nil
	}
	if resource == "_contrl" {
		return version, URL_CONTROL, nil
	}
	if resource == "_show" {
		return version, URL_SHOW, nil
	}
	if resource == "_debug" {
		return version, URL_DEBUG_SEARCH, nil
	}
	if resource == "_status" {
		return version, URL_SEARCH_STATUS, nil
	}
    if resource == "_load" {
		return version, URL_LOADDATA, nil
	}

	return -1, 0, errors.New("Error")

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