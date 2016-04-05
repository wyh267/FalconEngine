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
	"fmt"

	"github.com/huichen/sego"
)

type Segmenter struct {
	dictionary string
	segmenter  sego.Segmenter
    fssegmenter *FSSegmenter
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
    /*
	var seg sego.Segmenter
	this := &Segmenter{dictionary:dic_name, segmenter:seg}
	this.segmenter.LoadDictionary(dic_name)
	return this
    */
    this := &Segmenter{dictionary:dic_name} 
	this.fssegmenter = NewFSSegmenter(dic_name)
	if this == nil {
		fmt.Errorf("ERROR segment is nil")
		return nil
	}
	return this
}

func NewMyFSSegmenter(dic_name string) *Segmenter {
    this := &Segmenter{dictionary:dic_name} 
	this.fssegmenter = NewFSSegmenter(dic_name)
	if this == nil {
		fmt.Errorf("ERROR segment is nil")
		return nil
	}
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

/*
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
*/
    terms,_ := this.fssegmenter.Segment(content, search_mode)
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


func (this *Segmenter) FSSegmentWithTf(content string, search_mode bool) ([]TermInfo, int) {

	terms,_ := this.fssegmenter.Segment(content, search_mode)
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