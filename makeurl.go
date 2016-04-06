package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
    wordDic := make(map[string]bool)
	dicfile, err := os.Open("data/dictionary.txt")
	if err != nil {
		fmt.Printf("[ERROR] NewFSSegmenter :::: Open File[%v] Error %v\n", dicfile, err)
		return
	}
	defer dicfile.Close()

	scanner := bufio.NewScanner(dicfile)

	for scanner.Scan() {
		term := strings.Split(scanner.Text(), " ")
		wordDic[term[0]] = true
	}
    
    for k:=range wordDic {
        fmt.Printf("http://10.254.33.2:9990/v1/_search?index=weibo&q=%v&ps=10&pg=1&show=name,level,datetime,content\n",k)
    }
    
	return 

}
