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
	"net/http"
	"time"
	"utils"
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
	startTime = time.Now()
	result := make(map[string]interface{})

	if err != nil {
		this.Logger.Error(" %v", err)
	}

    this.engine.Search()
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
