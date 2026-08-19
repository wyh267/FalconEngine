package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/FalconEngine/falcon/index"
)

// newDocID 生成随机文档 ID（16 字节随机数 hex）
func newDocID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 几乎不可能发生，退化为时间戳
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// bulkItem bulk 响应中的单项结果
type bulkItem struct {
	Action string `json:"action"` // index / delete
	ID     string `json:"_id"`
	Status string `json:"status"` // ok / error
	Error  string `json:"error,omitempty"`
}

// handleBulk NDJSON 批量写入/删除：
//
//	{"index":{"_id":"1"}}\n{...doc...}\n{"index":{}}\n{...doc...}\n{"delete":{"_id":"2"}}\n
//
// action 行与文档行交替；delete 无文档行；index 省略 _id 时自动生成。
func (s *Server) handleBulk(w http.ResponseWriter, r *http.Request) {
	// 集群模式：索引存在性由 provider（路由表）校验
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

	lines := strings.Split(string(body), "\n")
	items := make([]bulkItem, 0, len(lines)/2)
	hasErr := false

	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		// action 行
		var action struct {
			Index *struct {
				ID string `json:"_id"`
			} `json:"index"`
			Delete *struct {
				ID string `json:"_id"`
			} `json:"delete"`
		}
		if err := json.Unmarshal([]byte(line), &action); err != nil || (action.Index == nil && action.Delete == nil) {
			writeError(w, http.StatusBadRequest, fmt.Errorf("第 %d 行不是合法的 action（index/delete）: %s", i+1, line))
			return
		}

		switch {
		case action.Index != nil:
			// 下一行是文档
			i++
			if i >= len(lines) || strings.TrimSpace(lines[i]) == "" {
				writeError(w, http.StatusBadRequest, fmt.Errorf("第 %d 行 index action 缺少文档行", i))
				return
			}
			doc := lines[i]
			id := action.Index.ID
			if id == "" {
				id = newDocID()
			}
			var err error
			if s.cluster != nil {
				err = s.cluster.WriteDoc(r.PathValue("index"), id, []byte(doc))
			} else {
				_, err = e.Index(id, []byte(doc))
			}
			if err != nil {
				hasErr = true
				items = append(items, bulkItem{Action: "index", ID: id, Status: "error", Error: err.Error()})
			} else {
				items = append(items, bulkItem{Action: "index", ID: id, Status: "ok"})
			}
		case action.Delete != nil:
			id := action.Delete.ID
			var ok bool
			var err error
			if s.cluster != nil {
				ok, err = s.cluster.DeleteDoc(r.PathValue("index"), id)
			} else {
				ok, _, err = e.Delete(id)
			}
			switch {
			case err != nil:
				hasErr = true
				items = append(items, bulkItem{Action: "delete", ID: id, Status: "error", Error: err.Error()})
			case !ok:
				hasErr = true
				items = append(items, bulkItem{Action: "delete", ID: id, Status: "error", Error: "文档不存在"})
			default:
				items = append(items, bulkItem{Action: "delete", ID: id, Status: "ok"})
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"errors": hasErr, "items": items})
}
