/*****************************************************************************
 *  file name : Builder.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 索引生成器
 *
******************************************************************************/


package builder


import (
	"BaseFunctions"
	"github.com/outmana/log4jzl"
)



type Builder struct{
	Configure	*BaseFunctions.Configure
	Dbadaptor   *BaseFunctions.DBAdaptor
	Logger            *log4jzl.Log4jzl
}



func NewBuilder(configure *BaseFunctions.Configure,dbadaptor *BaseFunctions.DBAdaptor,logger *log4jzl.Log4jzl) *Builder{
	this := &Builder{configure,dbadaptor,logger}
	return this
}


