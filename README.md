# FalconEngine（falcon）

用 Go 实现的现代化、插件化、分布式高可用搜索引擎（对搜索引擎感兴趣的可以去看看[这本书](http://www.amazon.cn/%E8%BF%99%E5%B0%B1%E6%98%AF%E6%90%9C%E7%B4%A2%E5%BC%95%E6%93%8E-%E6%A0%B8%E5%BF%83%E6%8A%80%E6%9C%AF%E8%AF%A6%E8%A7%A3-%E5%BC%A0%E4%BF%8A%E6%9E%97/dp/B006J9MSD8)）。

## 当前状态

**阶段 5 已完成 = 分布式高可用版本**（单机同样可独立部署使用）：

- 实时读写：文档写入即刻可查，translog（WAL）保证崩溃不丢数据
- 不可变段 + tiered 段合并，mmap 零拷贝读取
- BM25 相关度排序、多字段排序、数字/日期范围过滤
- ES 风格 JSON 查询 DSL 与聚合（terms/min/max/avg/sum/cardinality）
- 动态 mapping：未声明字段按 JSON 值类型自动推断
- 全插件化内核：字段类型、分词器、打分器、查询子句、聚合均可注册扩展
- REST API，进程内注册插件（同 database/sql 驱动模式）
- 分布式：分片 + 主备复制（push/pull）+ Raft 控制面选主 + 故障自动转移 + 跨节点 scatter-gather

## 快速开始

```bash
make build          # 构建 bin/falcon
make run            # 以 config.yaml 启动（无配置文件时用默认端口 9990、数据目录 ./data）
make demo           # 端到端演示：建索引→灌数据→查询→聚合→崩溃恢复
make test           # 全部单测
```

### 建索引

```bash
curl -X PUT http://127.0.0.1:9990/weibo -d '{"mappings":{"fields":[
  {"name":"datetime","type":"date"},
  {"name":"name","type":"keyword"},
  {"name":"level","type":"keyword"},
  {"name":"content","type":"text"},
  {"name":"likes","type":"number"}]}}'
```

`mappings` 可省略，省略后走纯动态 mapping（string→text、数字→number、bool→bool）。

### 写入文档

```bash
# 指定 ID
curl -X PUT http://127.0.0.1:9990/weibo/_doc/1 -d \
  '{"datetime":"2015-11-12 23:58:22","name":"延参法师","level":"黄V","content":"看山东，赞山东","likes":100}'

# 自动 ID
curl -X POST http://127.0.0.1:9990/weibo/_doc -d '{"content":"自动分配的文档"}'

# NDJSON 批量
printf '{"index":{"_id":"2"}}\n{"content":"批量写入"}\n{"delete":{"_id":"1"}}\n' | \
  curl -X POST http://127.0.0.1:9990/weibo/_bulk --data-binary @-
```

### 查询

```bash
# 全文检索（BM25）
curl -X POST http://127.0.0.1:9990/weibo/_search -d \
  '{"query":{"match":{"content":{"query":"雅礼中学","operator":"and"}}},"size":10}'

# bool + 日期范围过滤 + 排序 + 聚合
curl -X POST http://127.0.0.1:9990/weibo/_search -d '{
  "query":{"bool":{
    "must":[{"match":{"content":"雅礼"}}],
    "filter":[{"range":{"datetime":{"gte":"2013-08-18","lte":"2015-12-25"}}}]}},
  "sort":[{"likes":"desc"}],
  "from":0,"size":10,
  "aggs":{"by_level":{"terms":{"field":"level"}},"avg_likes":{"avg":{"field":"likes"}}}}'
```

## REST 端点

| 端点 | 说明 |
|---|---|
| `PUT /{index}` | 建索引（mappings 可省） |
| `DELETE /{index}` | 删除索引 |
| `PUT /{index}/_doc/{id}` | 写入/更新文档 |
| `POST /{index}/_doc` | 自动 ID 写入 |
| `GET /{index}/_doc/{id}` | 取原文 |
| `DELETE /{index}/_doc/{id}` | 删除文档 |
| `POST /{index}/_bulk` | NDJSON 批量（index/delete） |
| `POST\|GET /{index}/_search` | DSL 查询 |
| `POST /{index}/_refresh` | 手动刷新落盘（当前 refresh==flush） |
| `GET /_cluster/health` | 集群健康（单机桩） |
| `GET /_cat/indices` / `/_cat/shards` | 索引/分片列表 |

## 查询 DSL 支持矩阵

| 子句 | 说明 |
|---|---|
| `match` | 分词匹配，`operator`: or（默认）/and，可指定 `scorer` |
| `term` / `terms` | 精确匹配（keyword 走倒排，number/date/bool 走正排等值） |
| `range` | `gte/lte/gt/lt`，number/date/bool 正排过滤，date 支持 `"2006-01-02[ 15:04:05]"` |
| `bool` | `must`/`filter`（不打分）/`should`/`must_not`，可嵌套 |
| `match_all` / `ids` | 全量 / 按 ID 命中 |
| `sort` | `[{"field":"asc\|desc"}]`，含 `_score`，字段缺失排最后 |
| `aggs` | `terms`（精确桶）、`min`/`max`/`avg`/`sum`、`cardinality`（精确去重），两段式可分布式合并 |

## 字段类型

| 类型 | 倒排 | 正排 | 说明 |
|---|---|---|---|
| `text` | ✓（含 norms） | | 全文检索，standard 分词（ASCII 成词、CJK 单字） |
| `keyword` | ✓ | | 整词精确匹配 |
| `number` | | ✓ | 整数，过滤/排序 |
| `date` | | ✓ | 两种日期格式转 unix 秒 |
| `bool` | | ✓ | true/false 按 1/0 |
| `stored` | | | 仅存原文 |

## 插件开发

内核全部硬编码逻辑已插件化（注册表见 `plugin/` 包）：字段类型、分词器、打分器、查询子句、聚合。进程内注册（init 注册 + import 生效），重复注册 panic。

契约测试套件在 `plugin/plugintest/`，自定义插件可直接调用 `RunFieldTypeSuite` / `RunScorerSuite` 等验证。

完整示例见 [`examples/custom-plugin/`](examples/custom-plugin/)（翻转分词器 + 自定义字段类型）：

```go
import _ "github.com/FalconEngine/falcon/examples/custom-plugin/revtext" // init 自动注册
```

## 目录结构

```
cmd/falcon/    节点入口
api/           REST 服务层
index/         单机引擎（内存缓冲 + 段管理 + 查询执行 + IndexManager）
segment/       不可变段（落盘/读取/删除标记）
posting/       倒排索引（字典 + 分块倒排链）
dict/          有序字符串字典（稀疏索引）
docvalues/     正排列（number/keyword ord）
stored/        原文存储
translog/      预写日志（WAL，崩溃恢复）
analysis/      基础分词实现
schema/        字段结构（类型合法性由注册表决定）
plugin/        插件注册表与扩展点接口（plugintest 契约套件）
plugins/       内置插件（fieldtype/analyzer/scorer/query/agg）
storage/       存储后端（文件/mmap/内存）
pkg/           公共组件（mlog 日志、coding 帧编码）
examples/      自定义插件 demo
scripts/       demo.sh 端到端演示
legacy/        旧版实现（仅存档，不再维护）
```

## 分布式（阶段 5）

### 架构

```
┌────────────┐  gRPC(raft/复制/查询)  ┌────────────┐
│  master+data│ ◄──────────────────► │  master+data│  ...
│  (raft CSM) │ ◄─────────┐          │             │
└────────────┘           │          └────────────┘
       ▲                 │                 ▲
       └─────────────────┴─────────────────┘
        控制面：单 raft group 管理节点表/索引表/路由表
        数据面：primary-push + replica-pull 的 translog 复制
```

- **控制面**：单 raft group（etcd raft，MemoryStorage）管集群元数据：节点表、索引表、路由表。第一个 master 自举；后续 master 经 ConfChange 加入；data-only 节点经 `JoinNode` 注册并轮询状态。节点身份（grpc 地址派生）与 raft 成员 ID（每次启动唯一）分离，重启即新成员重新加入。
- **分片与路由**：建索引时均衡分配器按"持片数最少"放置 primary/replica（primary≠replica）；文档按 `murmur3(id) % numShards` 路由。
- **写入**：协调节点转发到 primary（`ForwardWrite`），primary 本地写后异步推送（`ApplyOp` 带 LSN 对齐）+ 副本按 LSN 拉取追赶（`FetchTranslog`）。`write_wait_for_active_shards: "all"` 时同步等全部存活副本 ack。
- **查询**：协调节点对每个分片本地优先、远端走 `ShardSearch`，归并排序 + 聚合两段式合并；分片不可用时降级为部分结果（`partial: true` + `failed_shards`）。
- **故障转移**：data 节点 1s 心跳上报分片 LSN；leader 6s 未收到判下线 → in-sync 副本升主（LSN 追平校验）→ 存活节点补建副本；旧 primary 重启后自动降级为副本并拉取追平。

### 集群配置示例

```yaml
node: {name: node2, master: true, data: true}
http: {port: 9992}
grpc: {port: 9993}
data: {path: ./data/node2}
cluster:
  name: falcon
  seeds: ["127.0.0.1:9991"]   # 首个节点不写 seeds（自举）
```

### 一键验证

```bash
make demo          # 单机全链路（含崩溃恢复）
make cluster-demo  # 3 节点：成员收敛/路由一致/跨节点查询
make chaos         # 混沌：kill primary → RTO 内恢复 → 数据不丢 → 重启追平
```

### 一致性语义

- 默认（`write_wait_for_active_shards: "1"`）：写 ack 即 primary 落盘+translog 刷盘；副本异步一致
- `"all"`：全部存活副本 ack 才返回；已 ack 数据在单 primary 故障后不丢
- 报错的写入可能已在 primary 落盘（at-least-once，语义同 ES）
- 读：默认查 primary（本地副本可读）；副本短暂滞后

## Roadmap

- [x] 阶段 1：骨架与存储基座（storage/dict/docvalues/translog）
- [x] 阶段 2：单机索引内核（倒排/正排/BM25/段合并/崩溃恢复）
- [x] 阶段 3：插件体系（注册表 + DSL + 两段式聚合 + 动态 mapping）
- [x] 阶段 4：REST API 与单机成品
- [x] 阶段 5a：分片化 + gRPC 节点间通信 + raft 控制面（元数据/路由表/均衡分配）
- [x] 阶段 5b：复制流（push/pull）+ 跨节点 scatter-gather + 心跳故障检测与转移

### 已知限制（后续阶段）

- raft 元数据不持久化（MemoryStorage）：全集群重启后需重注册；旧 raft 成员不摘除，长期运行 voter 膨胀
- 副本全量重建依赖 primary translog 未轮替（Flush 后历史被丢弃），落后太多的副本无法自愈——需快照传输
- 节点重启重放期间远端写可能短暂报错；删除索引暂不做集群级路由清理
