/*****************************************************************************
 *  file name : FSSegmenter.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 分词器（逆向最大匹配分词）
 *
******************************************************************************/

package utils

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type FSSegmenter struct {
	wordDic    map[string]bool
	stopWord   map[string]bool
	maxWordLen int
	//Logger     *Log4FE `json:"-"`
}

var stopwords string = "!@#$%^&*()_+=-`~,./<>?;':\"[]{}，。！￥……（）·；：「」『』、？《》】【“”|\\的"

func NewFSSegmenter(dicfilename string /*, logger *Log4FE*/) *FSSegmenter {

	this := &FSSegmenter{maxWordLen: 5, stopWord: make(map[string]bool), wordDic: make(map[string]bool)} //, Logger: logger}

	dicfile, err := os.Open(dicfilename)
	if err != nil {
		fmt.Printf("[ERROR] NewFSSegmenter :::: Open File[%v] Error %v\n", dicfile, err)
		return nil
	}
	defer dicfile.Close()

	scanner := bufio.NewScanner(dicfile)

	for scanner.Scan() {
		term := strings.Split(scanner.Text(), " ")
		this.wordDic[term[0]] = true
	}
	fmt.Printf("[INFO] Load Dictionary File [%v] OK\n", dicfilename)
	stopwordrune := []rune(stopwords)
	for _, stop := range stopwordrune {
		this.stopWord[string(stop)] = true
	}
	return this
}

func (this *FSSegmenter) subSegment(content []rune) []string {

	terms := make([]string, 0)
	end := len(content)
	for start := 1; start < end-1; start++ {
		subStr := string(content[start:end])
		if _, ok := this.wordDic[subStr]; ok {
			terms = append(terms, subStr)
		}

	}

	return terms

}

func (this *FSSegmenter) Segment(contentstr string, searchmode bool) ([]string, int) {

	//termInfos := make([]TermInfo,0)
	terms := make([]string, 0)
	content := []rune(contentstr)
	contentLen := len(content)
	maxLen := this.maxWordLen
	if maxLen > contentLen {
		maxLen = contentLen
	}

	s1 := content[:]
	start := contentLen - maxLen
	end := contentLen
	enwords := ""
	//this.Logger.Info("[INFO] Start: %v End : %v S1: %v", start, end, string(s1))
	for end > 0 {
		subStr := s1[start:end]
		subend := len(subStr)
		substart := 0
		moveLen := 0
		flag := false

		//this.Logger.Info("[INFO] substart: %v subend: %v substr: %v", substart, subend, string(subStr))
		for ; substart <= subend; substart++ {
			//this.Logger.Info("[INFO] substring %v", string(subStr[substart:subend]))
			if _, ok := this.wordDic[string(subStr[substart:subend])]; ok {
				if _, ok := this.stopWord[string(subStr[substart:subend])]; !ok {
					if enwords != "" {
						terms = append(terms, enwords)
						enwords = ""
					}
					terms = append(terms, string(subStr[substart:subend]))
					if searchmode && len(subStr) > 2 {
						//this.Logger.Info("[INFO] SubTerm %v", string(subStr[substart:subend]))
						subTerms := this.subSegment(subStr[substart:subend])
						terms = append(terms, subTerms...)
					}
				}
				moveLen = len(subStr[substart:subend])
				flag = true
				break
				//this.Logger.Info("[INFO] Term %v", string(subStr[substart:subend]))
			}
		}
		if !flag {
			start--
			end--
			sigleterm := string(subStr[len(subStr)-1 : len(subStr)])
			if _, ok := this.stopWord[sigleterm]; !ok {
				if len(sigleterm) == 1 {
					enwords = sigleterm + enwords
				} else {
					terms = append(terms, sigleterm)
				}
			}

		} else {
			start = start - moveLen
			end = end - moveLen
		}

		if start <= 0 {
			start = 0
		}

		if end == 1 && enwords != "" {
			terms = append(terms, enwords)
		}

	}

	//fmt.Printf("[INFO]  Terms :: %v\n", terms)

	return terms, 0

}
