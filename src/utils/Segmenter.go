/*****************************************************************************
 *  file name : Segmenter.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 分词器
 *
******************************************************************************/

package utils

import (
	"fmt"
	"github.com/huichen/sego"
)


type Segmenter struct{
	dictionary	string
	segmenter sego.Segmenter
}


/*****************************************************************************
*  function name : NewSegmenter
*  params : 
*  return : 
*
*  description : 
*
******************************************************************************/
func NewSegmenter(dic_name string) *Segmenter{
	var seg sego.Segmenter
	this := &Segmenter{dic_name,seg}
    this.segmenter.LoadDictionary(dic_name)
	return this
}


func (this *Segmenter)Segment(content string,search_mode bool) []string{

	text := []byte(content)
    segments := this.segmenter.Segment(text)
	res := sego.SegmentsToSlice(segments, search_mode)
	fmt.Println("SEGMENT::: ",res)
	return res
}




