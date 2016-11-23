package utils

import (
	"encoding/json"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/yanyiwu/gojieba"
)

var (
	STYLE_NORMAL       = 1
	STYLE_TONE         = 2
	STYLE_INITIALS     = 3
	STYLE_FIRST_LETTER = 4
	USE_SEGMENT        = true
	NO_SEGMENT         = false
	use_hmm            = true
	DICT_DIR           = "./data/"
	DICT_PHRASES       = "./data/phrases-dict"
)

var phrasesDict map[string]string
var reg *regexp.Regexp
var INITIALS []string = strings.Split("b,p,m,f,d,t,n,l,g,k,h,j,q,x,r,zh,ch,sh,z,c,s", ",")
var keyString string
var jieba *gojieba.Jieba
var sympolMap = map[string]string{
	"ā": "a1",
	"á": "a2",
	"ǎ": "a3",
	"à": "a4",
	"ē": "e1",
	"é": "e2",
	"ě": "e3",
	"è": "e4",
	"ō": "o1",
	"ó": "o2",
	"ǒ": "o3",
	"ò": "o4",
	"ī": "i1",
	"í": "i2",
	"ǐ": "i3",
	"ì": "i4",
	"ū": "u1",
	"ú": "u2",
	"ǔ": "u3",
	"ù": "u4",
	"ü": "v0",
	"ǘ": "v2",
	"ǚ": "v3",
	"ǜ": "v4",
	"ń": "n2",
	"ň": "n3",
	"": "m2",
}

func init() {
	keyString = getMapKeys()
	reg = regexp.MustCompile("([" + keyString + "])")

	//初始化时将gojieba实例化到内存
	jieba = gojieba.NewJieba()

	//初始化多音字到内存
	initPhrases()
}

func getMapKeys() string {
	keyString := ""
	for key, _ := range sympolMap {
		keyString += key
	}
	return keyString
}

func normalStr(str string) string {
	findRet := reg.FindString(str)
	return strings.Replace(str, findRet, string([]byte(sympolMap[findRet])[0]), -1)
}

func initPhrases() {
	f, err := os.Open(DICT_PHRASES)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&phrasesDict); err != nil {
		log.Fatal(err)
	}
}
