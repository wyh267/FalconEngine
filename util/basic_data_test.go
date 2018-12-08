package util

import (
	"testing"
	"github.com/FalconEngine/mlog"
)

func Test_BasicData(t *testing.T) {

	mlog.Start(mlog.LevelInfo,"")
	a:=int64(100)
	b:=UInt32(100)

	mlog.Info("%v",Equal(a,b))


}
