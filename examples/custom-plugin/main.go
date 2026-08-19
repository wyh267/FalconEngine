// custom-plugin demo：演示外部插件的完整接入流程。
//
// 运行：go run ./examples/custom-plugin
//
// 外部插件 revtext 注册了 "reverse" 分词器与 "reverse_text" 字段类型，
// 这里只需 import 它（init 自动注册），schema 中即可使用新字段类型。
// 索引与检索全程走插件逻辑：文档被翻转分词，查询词也被同样的分词器翻转，
// 因此用户用正常词检索即可命中（分词器对称性）。
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/FalconEngine/falcon/index"
	"github.com/FalconEngine/falcon/schema"

	// 关键一步：import 外部插件包，init() 自动完成注册
	_ "github.com/FalconEngine/falcon/examples/custom-plugin/revtext"
)

func main() {
	dir := filepath.Join(os.TempDir(), "falcon-custom-plugin-demo")
	defer os.RemoveAll(dir)

	// schema 中使用外部插件注册的字段类型
	sch, err := schema.New([]schema.Field{{Name: "content", Type: "reverse_text"}})
	if err != nil {
		panic(err)
	}
	e, err := index.Open(dir, sch)
	if err != nil {
		panic(err)
	}
	defer e.Close()

	// 索引两篇文档
	for id, doc := range map[string]string{
		"1": `{"content":"hello falcon"}`,
		"2": `{"content":"hello search"}`,
	} {
		if _, err := e.Index(id, []byte(doc)); err != nil {
			panic(err)
		}
	}

	// 索引中的 token 是翻转后的（"olleh"/"noclaf"/...），
	// 但查询词 "hello" 同样被翻转成 "olleh"，因此正常词即可命中
	res, err := e.SearchDSL([]byte(`{"query":{"match":{"content":"hello"}},"size":10}`))
	if err != nil {
		panic(err)
	}
	fmt.Printf("match \"hello\": total=%d\n", res.Total)
	for _, h := range res.Hits {
		fmt.Printf("  id=%s score=%.4f source=%s\n", h.ID, h.Score, h.Source)
	}

	// "falcon" 翻转后只命中文档 1
	res, _ = e.SearchDSL([]byte(`{"query":{"match":{"content":"falcon"}}}`))
	fmt.Printf("match \"falcon\": total=%d, first=%s\n", res.Total, res.Hits[0].ID)
}
