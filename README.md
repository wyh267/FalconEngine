# FalconEngine（falcon）

用 Go 实现的现代化、插件化、分布式高可用搜索引擎（对搜索引擎感兴趣的可以去看看[这本书](http://www.amazon.cn/%E8%BF%99%E5%B0%B1%E6%98%AF%E6%90%9C%E7%B4%A2%E5%BC%95%E6%93%8E-%E6%A0%B8%E5%BF%83%E6%8A%80%E6%9C%AF%E8%AF%A6%E8%A7%A3-%E5%BC%A0%E4%BF%8A%E6%9E%97/dp/B006J9MSD8)）。

## 当前状态

**阶段 5 已完成 = 分布式高可用版本**（单机同样可独立部署使用）：

- 实时读写：文档写入即刻可查，translog（WAL）保证崩溃不丢数据
- 不可变段 + tiered 段合并，mmap 零拷贝读取
- BM25 相关度排序、多字段排序、数字/日期范围过滤
- ES 风格 JSON 查询 DSL 与聚合（terms/min/max/avg/sum/cardinality）
- mapping 体系：multi-fields（text+keyword 双索引）、嵌套对象点路径展开、日期自动检测、动态 mapping + `PUT/GET _mapping` 显式管理
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

`mappings` 可省略，省略后走纯动态 mapping（数字→number、bool→bool、日期串→date、其余字符串→text 并附带 keyword 子字段）。

字段定义支持字段级 `analyzer` 覆盖（如 `{"name":"title","type":"text","analyzer":"whitespace"}`）与 multi-fields 子字段（一层为止）：

```bash
curl -X PUT http://127.0.0.1:9990/blog -d '{"mappings":{"fields":[
  {"name":"title","type":"text","fields":[{"name":"keyword","type":"keyword"}]}]}}'
# title 全文检索；title.keyword 整词精确匹配（term 查询 / 排序 / 聚合）
```

嵌套对象按点路径展开索引（`{"user":{"name":"x"}}` → `user.name`，数组不展开）；建索引声明的字段名不允许含 `.`。

mapping 显式管理（只增不改：新字段追加，已有字段不允许改类型，重复提交幂等；集群模式经 raft 广播到各节点）：

```bash
curl -X PUT http://127.0.0.1:9990/blog/_mapping -d '{"fields":[{"name":"tag","type":"keyword"}]}'
curl http://127.0.0.1:9990/blog/_mapping
```

动态 mapping 的日期检测可用索引设置关闭：`{"settings":{"date_detection":false}}`（默认 true）。

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
| `PUT /{index}/_mapping` | 更新 mapping（只增不改；改已有字段类型 400；幂等） |
| `GET /{index}/_mapping` | 查看当前 mapping |
| `PUT /{index}/_doc/{id}` | 写入/更新文档 |
| `POST /{index}/_doc` | 自动 ID 写入 |
| `GET /{index}/_doc/{id}` | 取原文 |
| `DELETE /{index}/_doc/{id}` | 删除文档 |
| `POST /{index}/_bulk` | NDJSON 批量（index/delete） |
| `POST\|GET /{index}/_search` | DSL 查询 |
| `POST /{index}/_refresh` | 手动刷新（缓冲实时可搜，轻量 no-op，仅作 API 兼容） |
| `POST /{index}/_flush` | 缓冲落盘为段并轮替 translog（重操作；缓冲达 1 万篇也会自动触发） |
| `GET /_cluster/health` | 集群健康（单机桩） |
| `GET /_cat/indices` / `/_cat/shards` | 索引/分片列表 |

## 查询 DSL 支持矩阵

| 子句 | 说明 |
|---|---|
| `match` | 分词匹配，`operator`: or（默认）/and，可指定 `scorer` |
| `match_phrase` | 短语匹配（依赖 v2 段 positions），slop>0 暂未支持 |
| `multi_match` | 多字段分词匹配，`fields:["title^3","content"]` 加权（should 并集加分，近似 best_fields） |
| `term` / `terms` | 精确匹配（keyword 走倒排，number/date/bool 走正排等值） |
| `prefix` / `wildcard` | 倒排字段词典前缀 / 通配符（`*`/`?`）匹配，恒定 1 分，展开上限 1024 |
| `fuzzy` | Levenshtein ≤2 模糊匹配，`fuzziness`（默认 2）、`max_expansions`（默认 50） |
| `exists` | 字段存在性过滤（空串 text、null 视为不存在） |
| `range` | `gte/lte/gt/lt`，number/date/bool 正排过滤，date 支持 `"2006-01-02[ 15:04:05]"` |
| `bool` | `must`/`filter`（不打分）/`should`/`must_not`，可嵌套 |
| `match_all` / `ids` | 全量 / 按 ID 命中 |
| `sort` | `[{"field":"asc\|desc"}]`，含 `_score`，字段缺失排最后 |
| `aggs` | `terms`（精确桶）、`min`/`max`/`avg`/`sum`、`cardinality`（精确去重），两段式可分布式合并 |

排序/聚合字段规则：**number/date/bool** 与 **keyword**（含 multi-fields 子字段如 `title.keyword`）字段可排序、可聚合；**text 本体不支持排序/聚合**（本引擎不做 fielddata），请使用其 keyword 子字段——动态 mapping 推断的字符串字段自动附带。命中结果携带 `sort` 排序键数组（语义同 ES，跨分片归并以此为据）。date 排序键为 unix 秒（精度到秒），bool 为 1/0。

## 字段类型

| 类型 | 倒排 | 正排 | 说明 |
|---|---|---|---|
| `text` | ✓（含 norms + positions） | | 全文检索，standard 分词（ASCII 成词、CJK 单字） |
| `keyword` | ✓ | ✓（ord 列） | 整词精确匹配 |
| `number` | | ✓ | 整数，过滤/排序 |
| `date` | | ✓ | 两种日期格式转 unix 秒 |
| `bool` | | ✓ | true/false 按 1/0 |
| `stored` | | | 仅存原文 |

## 分析链

分词器由三级组件组合而成（对标 ES）：**char filter → tokenizer → token filter**，经 `plugin.NewChainAnalyzer` 组合注册。内置组件：

| 类别 | 名称 | 说明 |
|---|---|---|
| tokenizer | `standard` / `keyword` / `whitespace` | ASCII 成词 CJK 单字 / 整词 / 按空白切 |
| token filter | `lowercase` / `stop` | 转小写 / 去英文停用词（保留 position 跳号） |
| char filter | `mapping` | 字符映射样板（`NewMappingFilter` 可自定义映射表） |

组合分词器：`standard`（standard+lowercase，默认）、`keyword`（整词不转小写）、`whitespace`（whitespace+lowercase）、`stop`（standard+lowercase+stop）。字段级覆盖示例：`{"name":"title","type":"text","analyzer":"stop"}`。

## 插件开发

内核全部硬编码逻辑已插件化（注册表见 `plugin/` 包）：字段类型、分析链组件（char filter/tokenizer/token filter）与分词器、打分器、查询子句、聚合。进程内注册（init 注册 + import 生效），重复注册 panic。

契约测试套件在 `plugin/plugintest/`，自定义插件可直接调用 `RunAnalyzerSuite` / `RunTokenizerSuite` / `RunTokenFilterSuite` / `RunCharFilterSuite` / `RunFieldTypeSuite` / `RunScorerSuite` 等验证。

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
scripts/       端到端演示与集群验证脚本（demo/cluster-demo/chaos/restart/recovery）
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

- **控制面**：单 raft group（etcd raft）管集群元数据：节点表、索引表、路由表。第一个 master 自举；后续 master 经 ConfChange 加入；data-only 节点经 `JoinNode` 注册并轮询状态。raft 日志/HardState/快照持久化在 `<data>/.falcon/raft/`（自研精简 WAL + 定期快照压缩），节点重启后从磁盘恢复、RaftID 每数据目录唯一并复用——全集群停机重启后元数据无需重注册即在。节点磁盘清空后以同地址重加入时，leader 检测出陈旧成员（同 NodeID 不同 RaftID），执行 remove_node 彻底清理（节点表/路由表摘除 + 确定性补建），并以一条 joint 成员变更原子替换 raft 成员。
- **分片与路由**：建索引时均衡分配器按"持片数最少"放置 primary/replica（primary≠replica）；文档按 `murmur3(id) % numShards` 路由。
- **写入**：协调节点转发到 primary（`ForwardWrite`），primary 本地写后异步推送（`ApplyOp` 带 LSN 对齐）+ 副本按 LSN 拉取追赶（`FetchTranslog`）。`write_wait_for_active_shards: "all"` 时同步等全部存活副本 ack。
- **副本恢复（peer recovery）**：translog 保留窗口（`translog-gens.json` 代际清单，primary 按副本 ack 推进全局检查点）让轻微落后的副本靠拉取自愈；落后出窗/历史分叉/primary 降级为副本三类情况触发"段拷贝 + translog 补差"两阶段恢复（`PrepareShardRecovery`/`FetchShardFile`/`FinishShardRecovery`），副本重建后从新 LSN 基线继续拉齐。恢复中的副本对 `"all"` 写入表现为不存在（语义同 ES initializing 分片）。
- **查询**：协调节点对每个分片本地优先、远端走 `ShardSearch`，归并排序 + 聚合两段式合并；分片不可用时降级为部分结果（`partial: true` + `failed_shards`）。
- **故障转移**：data 节点 1s 心跳上报分片 LSN；leader 6s 未收到判下线 → in-sync 副本升主（LSN 追平校验）→ 存活节点补建副本；旧 primary 重启后自动降级为副本并经段拷贝恢复重置（未复制的写入可丢，语义同 ES）。

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
make restart-test  # 全集群停机重启：元数据恢复/路由一致/数据可查/继续可写
make recovery-test # 副本恢复：kill 副本 → _flush 强制出窗 → 段拷贝追平、逐条一致
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
- [x] P0+P1 对标 ES 改造（raft 持久化与陈旧成员清理/副本段拷贝恢复/refresh 语义/删索引集群路由；段格式 v2/分析链三级化/新查询插件/mapping 体系/排序聚合收尾——明细见 [docs/es-roadmap.md](docs/es-roadmap.md)）

### 已知限制（后续阶段）

- 段格式 v2 为破坏性变更（无兼容期）：v1 老段可正常读（doc/freq/number 排序/聚合），但 match_phrase 与 keyword 字段排序遇 v1 段报错（排序器无法回落原文）——`_flush` 或等待段合并后自动重建为 v2；`FieldTypePlugin` 接口以 `DocValuesKind()` 取代 `DocValues()`，外部字段类型插件需同步适配
- `Analyzer` 接口破坏性变更（无兼容期）：`Tokenize(string) []string` 改为 `Analyze(string) []plugin.Token`（带 position）；外部分词器插件建议改为实现 `Tokenizer`/`TokenFilter`/`CharFilter` 组件后经 `plugin.NewChainAnalyzer` 组合注册（examples/custom-plugin 为样板）
- **mapping 广播窗口期**：primary 动态推断出新字段 → `CmdUpdateMapping` raft apply/轮询到达各节点之间存在窗口——窗口内收到含新字段文档的分片自行推断（可立即检索），未收到文档的分片暂时没有该字段；CSM mapping 到达后合并进本地分片、新写入正常，老段没有新字段的索引文件视为字段缺失、待段合并重建后自愈。同名字段在不同分片被推断为不同类型（如一处 `"2024-01-01"`→date、一处 `"abc"`→text）时先到者入 CSM，其余分片保留本地定义（类型分歧，日志告警）
- remove_node 仅清理非 leader 的陈旧成员（leader 自身陈旧属运维误操作，加入时返回错误提示）；节点被彻底摘除后不再自动均衡回分片（其空分片待后续建索引/rebalance 能力补齐）
- 段拷贝恢复期间源分片 `_flush`/`_merge` 报错（恢复会冻结 Flush/轮替）；恢复文件经 JSON codec 分块传输（大段拷贝慢，后续可换流式 RPC）；副本 ack 记账（retention lease）为内存态，primary 重启后退化为默认保留（只留当前代际），落后副本由段拷贝恢复兜底
- 节点重启重放期间远端写可能短暂报错
