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

func (this *Segmenter) SegmentSingle(content string) []string {

	rstr := []rune(content)
	res := make([]string, 0)
	for _, r := range rstr {
		res = append(res, string(r))

	}

	return res
}

func (this *Segmenter) SegmentWithSingle(content string) ([]TermInfo, int) {
	rstr := []rune(content)
	termmap := make(map[rune]bool)
	//pyl := GOptions.Convert(content)
	//var end int
	//if len(pyl) > 3 {
	//	end = 3
	//} else {
	//	end = len(pyl)
	//}
	//var py uint32 = 0

	//for i, p := range pyl[:end] {
	//	py = (uint32(p[0]) << uint32((end-1-i)*8)) | uint32(py)
	//}

	resterms := make([]TermInfo, 0)
	for _, r := range rstr {
		if _, ok := termmap[r]; !ok {
			resterms = append(resterms, TermInfo{Term: string(r), Tf: 0})
		}
		//termmap[r] = TermInfo{Term: string(r), Tf: int(py)}
	}

	//resterms := make([]TermInfo, len(termmap))
	//idx := 0
	//for _, tt := range termmap {
	//	resterms[idx] = tt
	//	idx++
	//}
	return resterms, len(rstr)

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

	/*
	   	segments := this.segmenter.Segment([]byte(content))
	   	if len(segments) == 0 {
	   		return nil, 0
	   	}
	   	//terms := make([]TermInfo, len(segments))
	       termmap:=make(map[string]TermInfo)
	       idx:=0
	   	for i := range segments {
	   		if _,ok:=termmap[segments[i].Token().Text()];ok{
	               t:=termmap[segments[i].Token().Text()]
	               t.Tf=t.Tf+1
	               termmap[segments[i].Token().Text()]=t
	               continue
	           }
	   		t:= TermInfo{Term:segments[i].Token().Text(),Tf:1}
	   		//terms[idx].Tf = 1//segments[i].Token().Frequency()
	           //termmap[terms[idx].Term]=true
	           termmap[segments[i].Token().Text()] =t
	           //fmt.Printf("[TREM:%v,FREQ:%v] ",terms[idx].Term,terms[idx].Tf)
	   		//idx++
	   	}
	       fmt.Printf("[TREM:%v] ",termmap)

	       terms := make([]TermInfo, len(termmap))
	       for _,tt:=range termmap{
	           terms[idx] = tt
	           idx++
	       }
	       //this.Segment(content,search_mode)
	   	//fmt.Println()
	   	return terms, len(segments)
	*/
}
