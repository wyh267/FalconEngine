package utils

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"github.com/apsdehal/go-logger"
)

type Log4FE struct {
	service_name  string
	service_env   string
	file_handle   *os.File
	logger_handle *logger.Logger
}

func New(service string) (log4FE *Log4FE, err error) {
	// TODO: 从配置文件里面读取：日志模式、日志路径、日志缓存
	filename := fmt.Sprintf("/var/log/FalconEngine/logs/%s.log", service, service)

	// 初始化Log4FE
	out, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		filename = fmt.Sprintf("%s.log", service)
		out, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
	}
	logtmp, _ := logger.New(filename, out)

	env_deploy := os.Getenv("ENV_DEPLOY")
	if env_deploy == "" {
		env_deploy = "TESTING"
	}

	log4FE = &Log4FE{
		service_name:  service,
		service_env:   env_deploy,
		file_handle:   out,
		logger_handle: logtmp,
	}

	return log4FE, nil
}

func (this *Log4FE) Close() (err error) {
	return this.file_handle.Close()
}

func (this *Log4FE) Abc(p1 string, p2 int) (err error) {
	fmt.Println(p1, p2)

	return nil
}

func (this *Log4FE) log(level string, format string, args ...interface{}) (err error) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(msg)

	_, filepath, filenum, _ := runtime.Caller(2)
	filename := path.Base(filepath)
	logmsg := fmt.Sprintf("%s %s %s %s %d - %s", this.service_name, this.service_env, level, filename, filenum, msg)
	this.logger_handle.Log("", logmsg)

	return nil
}

func (this *Log4FE) Fatal(format string, args ...interface{}) (err error) {
	return this.log("FATAL", format, args...)
}

func (this *Log4FE) Error(format string, args ...interface{}) (err error) {
	return this.log("ERROR", format, args...)
}

func (this *Log4FE) Warn(format string, args ...interface{}) (err error) {
	return this.log("WARN", format, args...)
}

func (this *Log4FE) Info(format string, args ...interface{}) (err error) {
	return this.log("INFO", format, args...)
}

func (this *Log4FE) Debug(format string, args ...interface{}) (err error) {
	return //this.log("DEBUG", format, args...)
}

func (this *Log4FE) Trace(format string, args ...interface{}) (err error) {
	return //this.log("TRACE", format, args...)
}
