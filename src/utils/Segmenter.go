/*****************************************************************************
 *  file name : Segmenter.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 分词器接口，目前封装了sego分词器
 *  github.com/huichen/sego
 *
******************************************************************************/

package utils

import (
	//"fmt"
	"github.com/huichen/sego"
	"strings"
)

type Segmenter struct {
	dictionary string
	segmenter  sego.Segmenter
}

/*****************************************************************************
*  function name : NewSegmenter
*  params :
*  return :
*
*  description :
*
******************************************************************************/
func NewSegmenter(dic_name string) *Segmenter {
	var seg sego.Segmenter
	this := &Segmenter{dic_name, seg}
	this.segmenter.LoadDictionary(dic_name)
	return this
}

func (this *Segmenter) Segment(content string, search_mode bool) []string {

	text := []byte(content)
	segments := this.segmenter.Segment(text)
	res := sego.SegmentsToSlice(segments, search_mode)
	//fmt.Println("SEGMENT::: ",res)
	return res
}



func (this *Segmenter) SegmentByType(content string, split_type int64,search_mode bool) []string {

	var terms []string
	switch split_type {
	case 1: //正常切词
		terms = RemoveDuplicatesAndEmpty(this.Segment(content, true))
	case 2: //按单个字符进行切词
		terms = RemoveDuplicatesAndEmpty(strings.Split(content, ""))
	case 3: //按规定的分隔符进行切词
		terms = RemoveDuplicatesAndEmpty(strings.Split(content, ";"))
	case 4: //按规定的分隔符进行切词
		terms = RemoveDuplicatesAndEmpty(strings.Split(content, "@"))
	}

	return terms
}