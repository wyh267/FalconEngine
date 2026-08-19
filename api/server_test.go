package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FalconEngine/falcon/index"
)

// newTestServer 创建测试服务（临时数据目录）
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	mgr, err := index.OpenManager(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(mgr).mux)
	t.Cleanup(func() {
		srv.Close()
		mgr.Close()
	})
	return srv
}

// do 发请求并返回状态码与解析后的 JSON
func do(t *testing.T, method, url, body string) (int, map[string]any) {
	t.Helper()
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	} else {
		rdr = strings.NewReader("")
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}
	return resp.StatusCode, out
}

func createWeiboIndex(t *testing.T, base string) {
	t.Helper()
	code, out := do(t, "PUT", base+"/weibo", `{"mappings":{"fields":[
		{"name":"datetime","type":"date"},
		{"name":"name","type":"keyword"},
		{"name":"level","type":"keyword"},
		{"name":"content","type":"text"},
		{"name":"likes","type":"number"}
	]}}`)
	if code != 200 {
		t.Fatalf("建索引失败: %d %v", code, out)
	}
}

func TestIndexLifecycle(t *testing.T) {
	srv := newTestServer(t)

	// 建索引（带 mappings）
	createWeiboIndex(t, srv.URL)

	// 重复建索引应报 400
	code, out := do(t, "PUT", srv.URL+"/weibo", "")
	if code != 400 || out["error"] == nil {
		t.Fatalf("重复建索引: code=%d out=%v", code, out)
	}

	// 非法索引名
	code, _ = do(t, "PUT", srv.URL+"/bad.name", "")
	// mux 按路径段匹配，bad.name 可作为 index 段，但管理器校验拒绝
	if code != 400 {
		t.Fatalf("非法索引名应 400, got %d", code)
	}

	// 非法字段类型
	code, _ = do(t, "PUT", srv.URL+"/badtype", `{"mappings":{"fields":[{"name":"x","type":"nope"}]}}`)
	if code != 400 {
		t.Fatalf("非法字段类型应 400, got %d", code)
	}

	// _cat/indices
	var indices []map[string]any
	resp, err := http.Get(srv.URL + "/_cat/indices")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("_cat/indices code=%d", resp.StatusCode)
	}
	json.NewDecoder(resp.Body).Decode(&indices)
	resp.Body.Close()
	if len(indices) != 1 || indices[0]["name"] != "weibo" {
		t.Fatalf("_cat/indices = %v", indices)
	}

	// 删除索引
	code, _ = do(t, "DELETE", srv.URL+"/weibo", "")
	if code != 200 {
		t.Fatalf("删除索引 code=%d", code)
	}
	// 再删应 404
	code, _ = do(t, "DELETE", srv.URL+"/weibo", "")
	if code != 404 {
		t.Fatalf("重复删除应 404, got %d", code)
	}
}

func TestDocCRUD(t *testing.T) {
	srv := newTestServer(t)
	createWeiboIndex(t, srv.URL)

	// 写入
	doc := `{"datetime":"2015-11-12 23:58:22","name":"延参法师","level":"黄V","content":"看山东，赞山东","likes":100}`
	code, out := do(t, "PUT", srv.URL+"/weibo/_doc/1", doc)
	if code != 200 || out["_id"] != "1" {
		t.Fatalf("写入失败: %d %v", code, out)
	}

	// 读取
	code, out = do(t, "GET", srv.URL+"/weibo/_doc/1", "")
	if code != 200 || out["found"] != true {
		t.Fatalf("读取失败: %d %v", code, out)
	}
	src := out["_source"].(map[string]any)
	if src["name"] != "延参法师" {
		t.Fatalf("原文不符: %v", src)
	}

	// 自动 ID 写入
	code, out = do(t, "POST", srv.URL+"/weibo/_doc", doc)
	if code != 201 || out["_id"] == "" {
		t.Fatalf("自动ID写入失败: %d %v", code, out)
	}

	// 更新（同 ID）
	code, _ = do(t, "PUT", srv.URL+"/weibo/_doc/1", strings.Replace(doc, `"likes":100`, `"likes":200`, 1))
	if code != 200 {
		t.Fatalf("更新失败: %d", code)
	}
	_, out = do(t, "GET", srv.URL+"/weibo/_doc/1", "")
	if out["_source"].(map[string]any)["likes"] != 200.0 {
		t.Fatalf("更新未生效: %v", out)
	}

	// 删除
	code, _ = do(t, "DELETE", srv.URL+"/weibo/_doc/1", "")
	if code != 200 {
		t.Fatalf("删除失败: %d", code)
	}
	code, out = do(t, "GET", srv.URL+"/weibo/_doc/1", "")
	if code != 404 {
		t.Fatalf("删除后读取应 404, got %d %v", code, out)
	}
	// 重复删除应 404
	code, _ = do(t, "DELETE", srv.URL+"/weibo/_doc/1", "")
	if code != 404 {
		t.Fatalf("重复删除应 404, got %d", code)
	}

	// 不存在的索引
	code, out = do(t, "PUT", srv.URL+"/nope/_doc/1", doc)
	if code != 404 || out["error"] == nil {
		t.Fatalf("不存在索引应 404: %d %v", code, out)
	}
}

func TestSearchEndpoint(t *testing.T) {
	srv := newTestServer(t)
	createWeiboIndex(t, srv.URL)

	bulk := ""
	docs := []string{
		`{"datetime":"2015-11-12 23:58:22","name":"延参法师","level":"黄V","content":"看山东，赞山东，和大家一起拉呱","likes":100}`,
		`{"datetime":"2015-11-13 08:00:00","name":"梦想家","level":"蓝V","content":"雅礼中学开始报名了","likes":50}`,
		`{"datetime":"2015-11-14 09:00:00","name":"路人甲","level":"普通用户","content":"可以回雅礼试试","likes":5}`,
	}
	for i, d := range docs {
		bulk += `{"index":{"_id":"` + string(rune('1'+i)) + `"}}` + "\n" + d + "\n"
	}
	code, out := do(t, "POST", srv.URL+"/weibo/_bulk", bulk)
	if code != 200 || out["errors"] != false {
		t.Fatalf("bulk 失败: %d %v", code, out)
	}
	if len(out["items"].([]any)) != 3 {
		t.Fatalf("bulk items = %v", out["items"])
	}

	// match 查询（内存缓冲立即可见）
	code, out = do(t, "POST", srv.URL+"/weibo/_search", `{"query":{"match":{"content":"雅礼"}},"size":10}`)
	if code != 200 || out["total"].(float64) != 2 {
		t.Fatalf("match 查询失败: %d %v", code, out)
	}

	// GET 带 body 查询也接受
	code, out = do(t, "GET", srv.URL+"/weibo/_search", `{"query":{"match":{"content":"雅礼"}}}`)
	if code != 200 || out["total"].(float64) != 2 {
		t.Fatalf("GET 查询失败: %d %v", code, out)
	}

	// bool + range + terms 聚合
	code, out = do(t, "POST", srv.URL+"/weibo/_search", `{
		"query":{"bool":{
			"must":[{"match":{"content":"雅礼"}}],
			"filter":[{"range":{"datetime":{"gte":"2015-11-13","lte":"2015-11-14 23:59:59"}}}]
		}},
		"aggs":{"by_level":{"terms":{"field":"level"}}}
	}`)
	if code != 200 || out["total"].(float64) != 2 {
		t.Fatalf("bool 查询失败: %d %v", code, out)
	}
	aggs := out["aggs"].(map[string]any)
	buckets := aggs["by_level"].(map[string]any)["buckets"].([]any)
	if len(buckets) != 2 {
		t.Fatalf("聚合 buckets = %v", buckets)
	}

	// refresh（落盘）后结果不变
	code, _ = do(t, "POST", srv.URL+"/weibo/_refresh", "")
	if code != 200 {
		t.Fatalf("refresh 失败: %d", code)
	}
	_, out = do(t, "POST", srv.URL+"/weibo/_search", `{"query":{"match":{"content":"雅礼"}}}`)
	if out["total"].(float64) != 2 {
		t.Fatalf("refresh 后查询结果变化: %v", out)
	}

	// 非法 DSL 应 400
	code, out = do(t, "POST", srv.URL+"/weibo/_search", `{"query":{"nope":{}}}`)
	if code != 400 || out["error"] == nil {
		t.Fatalf("非法 DSL 应 400: %d %v", code, out)
	}

	// 集群健康桩
	code, out = do(t, "GET", srv.URL+"/_cluster/health", "")
	if code != 200 || out["status"] != "green" {
		t.Fatalf("health = %d %v", code, out)
	}
}

func TestBulkDeleteAndError(t *testing.T) {
	srv := newTestServer(t)
	createWeiboIndex(t, srv.URL)

	// 写入后 bulk 删除，删除不存在的文档应标记 errors=true
	bulk := `{"index":{"_id":"1"}}
{"content":"你好"}
{"delete":{"_id":"1"}}
{"delete":{"_id":"999"}}
`
	code, out := do(t, "POST", srv.URL+"/weibo/_bulk", bulk)
	if code != 200 {
		t.Fatalf("bulk code=%d", code)
	}
	if out["errors"] != true {
		t.Fatalf("删除不存在文档应 errors=true: %v", out)
	}
	items := out["items"].([]any)
	if len(items) != 3 {
		t.Fatalf("items = %v", items)
	}
	if items[0].(map[string]any)["status"] != "ok" ||
		items[1].(map[string]any)["status"] != "ok" ||
		items[2].(map[string]any)["status"] != "error" {
		t.Fatalf("items 状态不符: %v", items)
	}

	// 坏 action 行应 400
	code, _ = do(t, "POST", srv.URL+"/weibo/_bulk", `{"foo":{"_id":"1"}}`+"\n")
	if code != 400 {
		t.Fatalf("坏 action 应 400, got %d", code)
	}
}

func TestDynamicMappingViaAPI(t *testing.T) {
	srv := newTestServer(t)

	// 无 mappings 建索引（纯动态 mapping）
	code, out := do(t, "PUT", srv.URL+"/dyn", "")
	if code != 200 {
		t.Fatalf("建索引失败: %d %v", code, out)
	}
	code, out = do(t, "PUT", srv.URL+"/dyn/_doc/1", `{"title":"动态字段","count":42}`)
	if code != 200 {
		t.Fatalf("写入失败: %d %v", code, out)
	}
	// 推断的 text 字段可检索
	code, out = do(t, "POST", srv.URL+"/dyn/_search", `{"query":{"match":{"title":"动态"}}}`)
	if out["total"].(float64) != 1 {
		t.Fatalf("动态 text 检索失败: %v", out)
	}
	// 推断的 number 字段可过滤
	_, out = do(t, "POST", srv.URL+"/dyn/_search", `{"query":{"range":{"count":{"gte":40}}}}`)
	if out["total"].(float64) != 1 {
		t.Fatalf("动态 number 过滤失败: %v", out)
	}
}
