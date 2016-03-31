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

import "github.com/huichen/sego"

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

func (this *Segmenter) SegmentWithTf(content string, search_mode bool) ([]TermInfo, int) {

	terms := this.Segment(content, search_mode)
	termmap := make(map[string]TermInfo)
	for _, term := range terms {
		if _, ok := termmap[term]; !ok {
			termmap[term] = TermInfo{Term: term, Tf: 1}
		} else {
			tf := termmap[term].Tf
			termmap[term] = TermInfo{Term: term, Tf: tf + 1}
		}
	}
	resterms := make([]TermInfo, len(termmap))
	idx := 0
	for _, tt := range termmap {
		resterms[idx] = tt
		idx++
	}
	//fmt.Printf("[TREMSSSSS::::%v] ",resterms)
	return resterms, len(terms)

	
}
