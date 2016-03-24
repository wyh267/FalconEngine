/*****************************************************************************
 *  file name : defaultEngine.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 数据层之上的引擎层
 *
******************************************************************************/

package FalconEngine

import "utils"

type DefaultEngine struct {
	Logger *utils.Log4FE    `json:"-"`
}

func NewDefaultEngine(logger *utils.Log4FE) *DefaultEngine {
	this := &DefaultEngine{Logger:logger}
	return this
}

func (this *DefaultEngine) Search() error {

    this.Logger.Info("[INFO] DefaultEngine Search >>>>>>>>")
	return nil
}
