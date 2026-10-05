// Package api 提供 falcon 的 REST 服务层。
//
// 端点：
//
//	PUT    /{index}                建索引（mappings 可省略 = 纯动态 mapping）
//	DELETE /{index}                删除索引
//	PUT    /{index}/_mapping       显式更新 mapping（{"fields":[...]}，只增不改）
//	GET    /{index}/_mapping       查看当前 mapping
//	PUT    /{index}/_doc/{id}      写入/更新文档
//	POST   /{index}/_doc           自动生成 ID 写入
//	GET    /{index}/_doc/{id}      取原文
//	DELETE /{index}/_doc/{id}      删除文档
//	POST   /{index}/_bulk          NDJSON 批量写入/删除
//	POST|GET /{index}/_search      DSL 查询
//	POST   /{index}/_refresh       手动刷新（缓冲实时可搜，refresh 为轻量 no-op）
//	POST   /{index}/_flush         缓冲落盘为段并轮替 translog（重操作）
//	GET    /_cluster/health        集群健康（单机桩，阶段5替换）
//	GET    /_cat/shards            分片列表（单机桩）
//	GET    /_cat/indices           索引列表与文档数
//
// 全部错误响应统一为 {"error": "...", "status": code}。
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/FalconEngine/falcon/index"
	"github.com/FalconEngine/falcon/pkg/mlog"
	"github.com/FalconEngine/falcon/schema"

	// REST 层直接面向用户，显式引入全部内置插件
	_ "github.com/FalconEngine/falcon/plugins"
)

// ClusterProvider 集群能力接口（由 node 装配层注入；单机测试可为 nil）
type ClusterProvider interface {
	// CreateIndex 集群感知建索引（leader 分配路由并 propose）
	CreateIndex(name string, settings index.IndexSettings, sch *schema.Schema) error
	// DeleteIndex 集群感知删索引（leader propose 后各节点清理本地数据）；
	// found=false 表示索引不存在
	DeleteIndex(name string) (bool, error)
	// UpdateMapping 集群感知 mapping 更新（leader 校验合并并 propose 广播；
	// 与现有字段定义冲突返回错误；幂等）
	UpdateMapping(name string, fields []schema.Field) error
	// ClusterState 集群状态快照（JSON）
	ClusterState() ([]byte, error)
	// WriteDoc 路由写入（协调 -> primary -> 复制）
	WriteDoc(indexName, id string, doc []byte) error
	// DeleteDoc 路由删除；found=false 表示文档不存在
	DeleteDoc(indexName, id string) (bool, error)
	// GetDoc 路由取原文
	GetDoc(indexName, id string) ([]byte, bool, error)
	// Search 集群级 scatter-gather 查询，返回序列化结果
	Search(indexName string, dsl []byte) ([]byte, error)
	// ClusterHealth 集群健康（green/yellow/red）
	ClusterHealth() ([]byte, error)
	// CatShards 分片列表
	CatShards() ([]byte, error)
}

// Server REST 服务
type Server struct {
	mgr     *index.Manager
	mux     *http.ServeMux
	srv     *http.Server
	cluster ClusterProvider // 非 nil 时启用集群感知路径
}

// NewServer 基于索引管理器创建 REST 服务
func NewServer(mgr *index.Manager) *Server {
	s := &Server{mgr: mgr, mux: http.NewServeMux()}
	s.routes()
	return s
}

// SetCluster 注入集群能力（node 装配层调用）
func (s *Server) SetCluster(c ClusterProvider) {
	s.cluster = c
	s.mux.HandleFunc("GET /_cluster/state", s.log(s.handleClusterState))
}

// routes 注册全部路由（Go 1.22+ 方法模式路由，零第三方依赖）
func (s *Server) routes() {
	s.mux.HandleFunc("PUT /{index}", s.log(s.handleCreateIndex))
	s.mux.HandleFunc("DELETE /{index}", s.log(s.handleDeleteIndex))
	s.mux.HandleFunc("PUT /{index}/_mapping", s.log(s.handlePutMapping))
	s.mux.HandleFunc("GET /{index}/_mapping", s.log(s.handleGetMapping))
	s.mux.HandleFunc("PUT /{index}/_doc/{id}", s.log(s.handlePutDoc))
	s.mux.HandleFunc("POST /{index}/_doc", s.log(s.handlePostDoc))
	s.mux.HandleFunc("GET /{index}/_doc/{id}", s.log(s.handleGetDoc))
	s.mux.HandleFunc("DELETE /{index}/_doc/{id}", s.log(s.handleDeleteDoc))
	s.mux.HandleFunc("POST /{index}/_bulk", s.log(s.handleBulk))
	s.mux.HandleFunc("POST /{index}/_search", s.log(s.handleSearch))
	s.mux.HandleFunc("GET /{index}/_search", s.log(s.handleSearch))
	s.mux.HandleFunc("POST /{index}/_refresh", s.log(s.handleRefresh))
	s.mux.HandleFunc("POST /{index}/_flush", s.log(s.handleFlush))
	s.mux.HandleFunc("GET /_cluster/health", s.log(s.handleClusterHealth))
	s.mux.HandleFunc("GET /_cat/shards", s.log(s.handleCatShards))
	s.mux.HandleFunc("GET /_cat/indices", s.log(s.handleCatIndices))
}

// Start 启动 HTTP 服务（阻塞）；addr 形如 ":9990"
func (s *Server) Start(addr string) error {
	s.srv = &http.Server{Addr: addr, Handler: s.mux}
	return s.srv.ListenAndServe()
}

// Shutdown 优雅退出
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

// ---------- 响应辅助 ----------

// writeJSON 写 JSON 响应
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError 统一错误响应 {"error": "...", "status": code}
func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error(), "status": status})
}

// statusWriter 包装 ResponseWriter 以记录状态码（日志用）
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// log 请求耗时日志中间件
func (s *Server) log(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next(sw, r)
		mlog.Info("http %s %s -> %d (%s)", r.Method, r.URL.Path, sw.status, time.Since(start))
	}
}

// engine 取索引，不存在时写 404 并返回 nil
func (s *Server) engine(w http.ResponseWriter, name string) *index.Index {
	e, ok := s.mgr.Get(name)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("索引 %q 不存在", name))
		return nil
	}
	return e
}

// readBody 读取请求体
func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, 64<<20)) // 上限 64MB
}

// ---------- 索引管理 ----------

// handleCreateIndex 建索引：body 可为空或
// {"settings":{"number_of_shards":N,"number_of_replicas":M},"mappings":{"fields":[...]}}
func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("index")
	body, err := readBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var sch *schema.Schema
	var settings index.IndexSettings
	if len(body) > 0 {
		var req struct {
			Settings index.IndexSettings `json:"settings"`
			Mappings struct {
				Fields []schema.Field `json:"fields"`
			} `json:"mappings"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("解析请求体失败: %w", err))
			return
		}
		settings = req.Settings
		if len(req.Mappings.Fields) > 0 {
			if sch, err = schema.New(req.Mappings.Fields); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
		}
	}
	// 集群模式：由 leader 分配路由并 propose；单机模式：本地直建
	if s.cluster != nil {
		if err := s.cluster.CreateIndex(name, settings, sch); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	} else if _, err := s.mgr.Create(name, settings, sch); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true, "index": name})
}

// handleDeleteIndex 删除索引（删除数据目录）。
// 集群模式：经 provider 走 raft 提案，各节点 apply/轮询后清理本地分片与数据目录；
// 单机模式：本地直删。
func (s *Server) handleDeleteIndex(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("index")
	var ok bool
	var err error
	if s.cluster != nil {
		ok, err = s.cluster.DeleteIndex(name)
	} else {
		ok, err = s.mgr.Delete(name)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("索引 %q 不存在", name))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true, "index": name})
}

// ---------- mapping ----------

// handlePutMapping 显式更新 mapping：body {"fields":[...]}。
// 新字段（含已有字段的新子字段）追加；与现有字段定义冲突返回 400；幂等。
// 字段名不允许含 '.'（与建索引声明一致，避免与嵌套对象点路径展开歧义）。
// 集群模式经 provider 走 leader 提案广播；单机模式直接合并进本地分片。
func (s *Server) handlePutMapping(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("index")
	body, err := readBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req struct {
		Fields []schema.Field `json:"fields"`
	}
	if err := json.Unmarshal(body, &req); err != nil || len(req.Fields) == 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("请求体应为 {\"fields\":[...]} 且字段列表非空"))
		return
	}
	for _, f := range req.Fields {
		if strings.Contains(f.Name, ".") {
			writeError(w, http.StatusBadRequest, fmt.Errorf("字段名 %q 不允许含 '.'（避免与嵌套对象的点路径展开歧义）", f.Name))
			return
		}
	}
	if s.cluster != nil {
		if err := s.cluster.UpdateMapping(name, req.Fields); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	} else {
		e := s.engine(w, name)
		if e == nil {
			return
		}
		if err := e.UpdateMapping(req.Fields); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true, "index": name})
}

// handleGetMapping 返回当前 mapping：{"index":..., "mappings":{"fields":[...]}}。
// 单机/本地持有分片时返回本地各分片 schema 并集；集群协调节点不持有分片时
// 回落到集群状态（CSM）中的 mapping（权威）。
func (s *Server) handleGetMapping(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("index")
	if ix, ok := s.mgr.Get(name); ok {
		writeJSON(w, http.StatusOK, map[string]any{
			"index":    name,
			"mappings": map[string]any{"fields": ix.UnionSchema().Fields},
		})
		return
	}
	if s.cluster != nil {
		state, err := s.cluster.ClusterState()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		var snap struct {
			Indices map[string]struct {
				Mapping json.RawMessage `json:"mapping"`
			} `json:"indices"`
		}
		if err := json.Unmarshal(state, &snap); err == nil {
			if meta, ok := snap.Indices[name]; ok {
				var mapping json.RawMessage = meta.Mapping
				if len(mapping) == 0 {
					mapping = json.RawMessage(`{"fields":[]}`)
				}
				writeJSON(w, http.StatusOK, map[string]any{"index": name, "mappings": mapping})
				return
			}
		}
	}
	writeError(w, http.StatusNotFound, fmt.Errorf("索引 %q 不存在", name))
}

// ---------- 文档读写 ----------

// handlePutDoc 写入/更新指定 ID 的文档
func (s *Server) handlePutDoc(w http.ResponseWriter, r *http.Request) {
	// 集群模式：索引存在性由 provider（路由表）校验，协调节点可不持有分片
	var e *index.Index
	if s.cluster == nil {
		e = s.engine(w, r.PathValue("index"))
		if e == nil {
			return
		}
	}
	id := r.PathValue("id")
	body, err := readBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if s.cluster != nil {
		if err := s.cluster.WriteDoc(r.PathValue("index"), id, body); err != nil {
			mlog.Error("WriteDoc 失败 id=%s body=%q err=%v", id, string(body), err)
			writeError(w, http.StatusBadRequest, err)
			return
		}
	} else if _, err := e.Index(id, body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"_id": id, "result": "indexed"})
}

// handlePostDoc 自动生成 ID 写入
func (s *Server) handlePostDoc(w http.ResponseWriter, r *http.Request) {
	// 集群模式：索引存在性由 provider（路由表）校验，协调节点可不持有分片
	var e *index.Index
	if s.cluster == nil {
		e = s.engine(w, r.PathValue("index"))
		if e == nil {
			return
		}
	}
	body, err := readBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id := newDocID()
	if s.cluster != nil {
		err = s.cluster.WriteDoc(r.PathValue("index"), id, body)
	} else {
		_, err = e.Index(id, body)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"_id": id, "result": "created"})
}

// handleGetDoc 取原文
func (s *Server) handleGetDoc(w http.ResponseWriter, r *http.Request) {
	// 集群模式：索引存在性由 provider（路由表）校验，协调节点可不持有分片
	var e *index.Index
	if s.cluster == nil {
		e = s.engine(w, r.PathValue("index"))
		if e == nil {
			return
		}
	}
	id := r.PathValue("id")
	var raw json.RawMessage
	var found bool
	var err error
	if s.cluster != nil {
		b, f, e2 := s.cluster.GetDoc(r.PathValue("index"), id)
		raw, found, err = json.RawMessage(b), f, e2
	} else {
		raw, found, err = e.Get(id)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, fmt.Errorf("文档 %q 不存在", id))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"_id": id, "found": true, "_source": raw})
}

// handleDeleteDoc 删除文档
func (s *Server) handleDeleteDoc(w http.ResponseWriter, r *http.Request) {
	// 集群模式：索引存在性由 provider（路由表）校验，协调节点可不持有分片
	var e *index.Index
	if s.cluster == nil {
		e = s.engine(w, r.PathValue("index"))
		if e == nil {
			return
		}
	}
	id := r.PathValue("id")
	var ok bool
	var err error
	if s.cluster != nil {
		ok, err = s.cluster.DeleteDoc(r.PathValue("index"), id)
	} else {
		ok, _, err = e.Delete(id)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("文档 %q 不存在", id))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"_id": id, "result": "deleted"})
}

// ---------- 搜索 ----------

// handleSearch DSL 查询（POST/GET 均可，body 为 DSL JSON）
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	// 集群模式：索引存在性由 provider（路由表）校验，协调节点可不持有分片
	var e *index.Index
	if s.cluster == nil {
		e = s.engine(w, r.PathValue("index"))
		if e == nil {
			return
		}
	}
	body, err := readBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(body) == 0 {
		body = []byte(`{}`)
	}
	if s.cluster != nil {
		res, err := s.cluster.Search(r.PathValue("index"), body)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(res)
		return
	}
	res, err := e.SearchDSL(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleRefresh 手动刷新。本引擎内存缓冲实时可搜（写入即可查，
// 契约见 index.TestBufferRealtimeVisibility），refresh 无需任何动作，
// 返回 ack 保持 API 兼容；需要落盘持久化请用 _flush。
func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	// 集群模式：协调节点可能不持有该索引分片，无法本地校验存在性，直接 ack
	if s.cluster != nil {
		writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true})
		return
	}
	if e := s.engine(w, r.PathValue("index")); e == nil {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true})
}

// handleFlush 手动落盘：缓冲写为新段并轮替 translog 代际（重操作）。
// 集群模式：flush 本节点持有的该索引分片（协调节点可能不持有，视为成功）。
// 分片作为恢复源被段拷贝期间 Flush 被冻结，本接口返回 500（恢复完成后自愈）。
func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request) {
	if s.cluster != nil {
		if ix, ok := s.mgr.Get(r.PathValue("index")); ok {
			if err := ix.Flush(); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true})
		return
	}
	e := s.engine(w, r.PathValue("index"))
	if e == nil {
		return
	}
	if err := e.Flush(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true})
}

// ---------- 集群/索引状态（单机桩） ----------

// handleClusterHealth 集群健康：green/yellow/red
func (s *Server) handleClusterHealth(w http.ResponseWriter, r *http.Request) {
	if s.cluster != nil {
		state, err := s.cluster.ClusterHealth()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(state)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "green", "nodes": 1})
}

// handleClusterState 返回集群元数据快照（raft CSM）
func (s *Server) handleClusterState(w http.ResponseWriter, r *http.Request) {
	state, err := s.cluster.ClusterState()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write(state)
}

// handleCatShards 分片列表：集群模式展示真实路由与分片状态
func (s *Server) handleCatShards(w http.ResponseWriter, r *http.Request) {
	if s.cluster != nil {
		rows, err := s.cluster.CatShards()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(rows)
		return
	}
	type shard struct {
		Index string `json:"index"`
		Shard int    `json:"shard"`
		Docs  int    `json:"docs"`
	}
	out := []shard{}
	for _, info := range s.mgr.List() {
		out = append(out, shard{Index: info.Name, Shard: 0, Docs: info.Docs})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleCatIndices 索引列表与文档数
func (s *Server) handleCatIndices(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.mgr.List())
}
