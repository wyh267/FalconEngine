/*****************************************************************************
 *  file name : Node.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 分布式节点
 *
******************************************************************************/

package FalconNode

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"
	"utils"
)

const (
	URL_CLUSTER_JOIN uint64 = 1
)

type Node struct {
	IsMaster bool          `json:"is_master"`
	port     int           `json:"node_port"`
	Logger   *utils.Log4FE `json:"-"`
}

func NewNode(isMaster bool, nodeport int, logger *utils.Log4FE) *Node {

	this := &Node{IsMaster: isMaster, Logger: logger, port: nodeport}

	return this

}

func (this *Node) AddMaster(ip string, port int) error {
	this.masterIp = ip
	this.masterPort = port

	return nil

}

func (this *Node) StartNode() error {

	addr := fmt.Sprintf(":%d", this.port)
	err := http.ListenAndServe(addr, this)
	if err != nil {
		this.Logger.Error("Node Server start fail: %v", err)
		return err
	}
	this.Logger.Info("Node Server started")

	if this.IsMaster {

	} else {
		//寻找主节点

	}

	return nil

}

func (this *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {

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
		return
	}

	if this.IsMaster {
		//新节点加入
		switch reqType {
		case URL_CLUSTER_JOIN:

		default:
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, MakeErrorResult(-1, err.Error()))
			return
		}

		//旧节点复活

	} else {
		//回复主节点信息

		//回复心跳

		//回复本节点信息

	}

}

func (this *Node) parseArgs(r *http.Request) (map[string]string, error) {
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
func (this *Node) ParseURL(url string) (int, uint64, error) {
	//确定是否是本服务能提供的控制类型

	urlPattern := "/v(\\d)/cluster/(_join)\\?"
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
	if resource == "_join" {
		return version, URL_CLUSTER_JOIN, nil
	}

	return -1, 0, errors.New("Error")

}
