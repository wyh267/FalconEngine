package utils

import (
	"strings"
	"unicode/utf8"
)

type options struct {
	style     int
	segment   bool
	heteronym bool
}

var GOptions *options

func (this *options) perStr(pinyinStrs string) string {
	switch this.style {
	case STYLE_INITIALS:
		for i := 0; i < len(INITIALS); i++ {
			if strings.Index(pinyinStrs, INITIALS[i]) == 0 {
				return INITIALS[i]
			}
		}
		return ""
	case STYLE_TONE:
		ret := strings.Split(pinyinStrs, ",")
		return ret[0]
	case STYLE_NORMAL:
		ret := strings.Split(pinyinStrs, ",")
		return normalStr(ret[0])
	}
	return ""
}

func (this *options) doConvert(strs string) []string {
	//获取字符串的长度
	bytes := []byte(strs)
	pinyinArr := make([]string, 0)
	nohans := ""
	var tempStr string
	var single string
	for len(bytes) > 0 {
		r, w := utf8.DecodeRune(bytes)
		bytes = bytes[w:]
		single = get(int(r))
		// 中文字符判断
		tempStr = string(r)
		if len(single) == 0 {
			nohans += tempStr
		} else {
			if len(nohans) > 0 {
				pinyinArr = append(pinyinArr, nohans)
				nohans = ""
			}
			pinyinArr = append(pinyinArr, this.perStr(single))
		}
	}
	//处理末尾非中文的字符串
	if len(nohans) > 0 {
		pinyinArr = append(pinyinArr, nohans)
	}
	return pinyinArr
}
func (this *options) Convert(strs string) []string {
	retArr := make([]string, 0)
	if this.segment {
		jiebaed := jieba.Cut(strs, use_hmm)
		for _, item := range jiebaed {
			mapValuesStr, exist := phrasesDict[item]
			mapValuesArr := strings.Split(mapValuesStr, ",")
			if exist {
				for _, v := range mapValuesArr {
					retArr = append(retArr, this.perStr(v))
				}
			} else {
				converted := this.doConvert(item)
				for _, v := range converted {
					retArr = append(retArr, v)
				}
			}
		}
	} else {
		retArr = this.doConvert(strs)
	}

	return retArr
}

func NewPy(style int, segment bool) *options {
	GOptions := &options{style, segment, false}
	return GOptions
}
