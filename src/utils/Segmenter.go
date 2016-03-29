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

import
//"fmt"
"github.com/huichen/sego"

type Segmenter struct {
	dictionary string
	segmenter  sego.Segmenter
}

// GSegmenter 分词器全局变量
var GSegmenter *Segmenter

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



func (this *Segmenter) SegmentWithTf(content string,search_mode bool) ([]TermInfo,int) {
    
	segments := this.segmenter.Segment([]byte(content))
    if len(segments) == 0 {
        return nil,0
    }
    terms := make([]TermInfo,len(segments))
    sumTermCount := 0
    for i:=range terms{
        terms[i].Term=segments[i].Token().Text()
        terms[i].Tf=segments[i].Token().Frequency()
        sumTermCount += terms[i].Tf
    }
	
	return terms,sumTermCount
    
    
}