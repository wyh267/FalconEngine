package utils

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func Test_Segmenter(t *testing.T) {
	logger, _ := New("test_log")
	var err error
	mysegmenter := NewMyFSSegmenter("/Users/wuyinghao/Desktop/FalconEngine/data/dictionary.txt")
	if mysegmenter == nil {
		logger.Error("ERROR....")
		t.Error("Fail...")
	}

	mysegmenter.FSSegmentWithTf("中华人民共和国中央人民政府", true)

	datafile, err := os.Open("/Users/wuyinghao/Desktop/FalconSearch/w1w.log")
	if err != nil {
		t.Error("Fail...", err)
	}
	defer datafile.Close()

	scanner := bufio.NewScanner(datafile)
	start := time.Now()
	for scanner.Scan() {
		sptext := strings.Split(scanner.Text(), "\t")
		if len(sptext) != 4 {
			continue
		}
		mysegmenter.FSSegmentWithTf(sptext[3], true)
        
        
		//fmt.Println(sptext)
	}
	fmt.Println(time.Now().Sub(start))
	if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}

}



func Test_SegoSegmenter(t *testing.T) {
	logger, _ := New("test_log")
	var err error
	mysegmenter := NewSegmenter("/Users/wuyinghao/Desktop/FalconEngine/data/dictionary.txt")
	if mysegmenter == nil {
		logger.Error("ERROR....")
		t.Error("Fail...")
	}

	mysegmenter.SegmentWithTf("中华人民共和国中央人民政府", true)

	datafile, err := os.Open("/Users/wuyinghao/Desktop/FalconSearch/w1w.log")
	if err != nil {
		t.Error("Fail...", err)
	}
	defer datafile.Close()

	scanner := bufio.NewScanner(datafile)
	start := time.Now()
	for scanner.Scan() {
		sptext := strings.Split(scanner.Text(), "\t")
		if len(sptext) != 4 {
			continue
		}
		mysegmenter.SegmentWithTf(sptext[3], true)
		//fmt.Println(sptext)
	}
	fmt.Println(time.Now().Sub(start))
	if err != nil {
		t.Error("Fail...", err)
	} else {
		t.Log("UnSubscribeEmail OK")
	}

}