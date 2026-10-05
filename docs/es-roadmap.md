# FalconEngine 对标 Elasticsearch 改造路线图

> 记录日期：2026-10-05
> 排序逻辑：先解决**数据丢失/结果错误**类的硬伤（最致命），再补**检索核心能力**（功能缺失，可逐步补），最后补**性能运维与生态**（体验差距）。

## P0：正确性与可用性基石

不改这些，对标无从谈起。四项相互耦合，建议作为一个整体推进。

### P0-1 cluster/raft 元数据持久化
- ~~现状：raft 使用 `MemoryStorage`，全集群重启后节点表、路由表、分片状态全丢（`cluster/raft.go:17-18` 注释明确承认该简化）。~~
- **已完成（2026-10-05）**：自研精简 WAL + 快照持久化层（`cluster/store.go`，与 translog 同构：复用 pkg/coding CRC 帧 + storage.FileWriter，零新增依赖），落盘到 `<data>/.falcon/raft/`（node.json / wal-\<n\>.log / snap-\<index\>.json）；RaftNode Ready 循环按 etcd 标准顺序接入（快照落盘→ApplySnapshot→CSM Restore → WAL 追加→MemoryStorage → HardState 落盘 → 发送 → apply → Advance），应用进度领先快照 512 条触发 `CreateSnapshot` + WAL 轮替 + Compact（先 snap 落盘再截断 WAL，重叠合法幂等）；RaftID 语义改为"每数据目录唯一"并先落盘再启动 raft，重启后 `RestartNode` 从磁盘恢复、跳过 joinViaSeeds，全集群停机重启元数据无需重注册即在；陈旧成员彻底清理（remove_node）：同 NodeID（同 grpc 地址）不同 RaftID 加入时判定旧成员磁盘已清，leader 先提案 `CmdRemoveNode`（节点表/Dead 表/分片状态摘除、路由表其分片摘除并确定性补建，含无副本时的空 primary 分配），再以一条 joint 成员变更原子移除旧 raft 成员并加入新成员；仅清理非 leader 陈旧成员（指向 leader 自身属运维误操作，报错）。顺带修复两个潜伏 bug：etcd raft v3.7 `StartNode` 自举写 V1 格式成员变更条目（旧代码只处理 V2，follower 重放后 tracker 缺自举成员，快照后重启即脑裂），以及 raft 消息未校验收件人 ID（清盘重加入节点会吃进发给旧成员的日志造成污染）。
- 验证：`cluster/store_test.go`（WAL 写读/尾部半帧截断容忍/快照落盘轮替回环）、`cluster/cluster_test.go`（3 节点持久化全停全启：CSM 逐项相等/选主/新提案复制；remove_node 后成员表/路由表正确）、`node/cluster_integration_test.go`（TestClusterFullRestart 全停全启元数据在/文档可查/新写入追平；TestClusterWipedNodeRejoin 磁盘清空重加入旧成员被清理）、`scripts/cluster-restart-test.sh` + `make restart-test`。

### P0-2 node/replication 副本恢复机制重构
- ~~现状：副本追平完全依赖 primary 的 translog 未轮替，落后太多永远无法自愈（`index/engine.go:351`）。~~
- 目标：对标 ES 的 sequence number + 全局检查点 + translog retention lease + 基于段的 peer recovery（段拷贝 + translog 补差两阶段）。
- **已完成（2026-10-05）**：LSN 即 sequence number（不另起炉灶）。① translog 保留窗口：新 manifest `<shard>/translog-gens.json`（`[{gen, base_lsn, count}]`，Flush 全量重写、先落清单再删文件），Flush 轮替后旧代际按保留地板清除（默认只留当前代际，单机不膨胀；无 manifest 的老数据目录按现状只认最新代际，向后兼容），`ReadTranslog` 跨代际连读，`Engine.SetRetentionLSN/OldestLSN`。② 副本检查点（retention lease 最小形态）：primary 内存态 `replAcks`（ApplyOp 响应 + FetchTranslog 拉取位点两个来源刷新），全局检查点 = min(存活副本 ack)（未见过的存活副本保守 -1；死亡副本租约释放，对齐 ES），随 ack/对账推进 `SetRetentionLSN`。③ 两阶段恢复：引擎侧 `PrepareRecovery`（冻结 Flush/轮替/合并，捕获一致段文件清单，del.bin 按 Prepare 时刻前缀拷贝、差量由 translog 重放补齐）/`FinishRecovery`/`RecoverShard`（备份旧目录→重建→写 LSN 基线→重开，失败回滚）；三个新 RPC（`PrepareShardRecovery`/`FetchShardFile` 1MB 分块/`FinishShardRecovery`，JSON codec 对 []byte 自动 base64）；三个触发点——落后出窗（FetchTranslogResponse 带 `OldestLsn`，fromLSN < oldest 返回空 ops）、历史分叉（空 ops 且 nextLSN < fromLSN）、角色降级（reconcile 借分片目录 `primary-role` 标记识别 primary→replica，含进程重启后，规避"同 LSN 不同内容"静默分叉；比计划书的内存 lastRole map 更强，标记随副本重建消失）。恢复中副本对 waitAll 表现为不存在（语义同 ES initializing 分片）；恢复期间 `_flush`/`_merge` 报错（文档化），该分片查询走已有 partial 降级。
- 验证：`index/engine_test.go`（保留窗口推进/跨代际连读/manifest 重启保留；Prepare→Recover 跨目录 round-trip 含新旧段混合与缓冲差量、恢复期间 Flush 报错、Finish 后恢复、拉取失败回滚）、`node/recovery_test.go`（场景 A 出窗：kill 副本→持续写+6 轮 _flush→出窗断言→重启后段拷贝追平、23 篇逐条一致——已验证去掉恢复触发后此用例必失败；场景 B 降级分叉：幻影写后立即 kill primary→故障转移→新 primary 续写越过幻影位点→旧 primary 重启被重置、无幻影文档）、`scripts/recovery-test.sh` + `make recovery-test`。
- 遗留：恢复文件经 JSON codec 分块传输（大段慢，可换流式 RPC）；replAcks 内存态（primary 重启退化默认保留，副本走段拷贝兜底，安全）。

### P0-3 index refresh 语义重构（NRT 搜索）
- ~~现状：`_refresh` 是 Flush 的别名（`api/server.go:378-381`），每次刷新落一个完整新段，既慢又放大段合并压力。~~
- **已完成（2026-10-05）**：核实内存缓冲本事实时可搜（全部查询路径均查 memBuf），`_refresh` 改为轻量 no-op（仅 ack）；落盘能力拆为独立 `POST /{index}/_flush`（单机 flush 本地分片，集群 flush 本节点持有分片）。`index.Engine` 新增缓冲文档数阈值 `maxBufferDocs`（默认 10000），超限由写入侧持锁内联 flush，防止缓冲无界增长；触发时写入阻塞一个落盘周期为已知权衡（异步 flush 留待后续）。

### P0-4 集群模式写操作正确性漏洞修复
- ~~现状：删索引不走集群路由清理、删文档是本地操作不复制（`api/server.go` 的 handleDeleteIndex 未走 ClusterProvider），集群模式下直接导致数据不一致，属 bug 级。~~
- 事实修正：删文档**已经**走集群复制链路（`api/server.go` handleDeleteDoc → `node/replication.go` DeleteDoc → writeOp 路由 + primary 推送/副本拉取）；真实缺口只剩删索引（`CmdDeleteIndex` 已定义但无人 propose）。
- **已完成（2026-10-05）**：删索引经 leader raft 提案 `CmdDeleteIndex` 广播；master 节点在 raft apply 回调、data-only 节点在轮询对账中调用 `deleteLocalIndex`（停拉取器、清推送队列、关引擎删数据目录）；REST `DELETE /{index}` 集群模式改走 `ClusterProvider.DeleteIndex`（found=false → 404）。

## P1：检索核心能力差距

### P1-5 posting 结构支持 positions/offsets
- ~~现状：倒排只存 `(docID, tf)`（`posting/posting.go`），做不了短语查询（match_phrase）与高亮。~~
- **已完成（2026-10-05，段格式 v2 一部分）**：posting v2 头部加 `[uvarint flags]`（bit0=hasPositions），块内记录扩展为 `[docDelta][freq][freq × posDelta]`，块索引区不变（Advance 跳读零改动）；`FieldWriter.Add(term, docID, positions)`、迭代器新增 `Positions()`（v1 段返回 nil）；内存缓冲同步改存 positions（token 下标即位置）。match_phrase 执行器落地（IR `plugin.PhraseNode` + `index/search.go` evalPhraseLocked：最小 df 主驱动 + Advance 对齐 + 连续位置验证，打分为各 term BM25 之和、短语 tf 不参与，与 ES 有差异）；DSL 解析器与 slop>0 待阶段 8。offsets 未做（高亮需要时再扩 flags）。
- v1 段读兼容（doc/freq 可查）；短语查询遇 v1 段报带 `_flush`/merge 指引的明确错误，merge 从原文重解析即自动升级 v2。

### P1-6 查询与分析链扩展
- ~~现状：缺 `match_phrase / multi_match / wildcard / prefix / fuzzy / exists`；分析器只有 tokenizer 一层。~~
- 目标：对标 ES 的 char filter → tokenizer → token filter 三级分析链；新增查询以插件形式扩展（`plugin/registry.go` 已有扩展点，属"加插件"而非重构）。
- **已完成（2026-10-05，分析链部分）**：① 接口三级化（破坏性变更，无兼容期）：`plugin.Token{Term, Position, Start, End}`（Start/End 为 offsets 预留，本期不填），`Analyzer` 改为 `Analyze(string) []Token`，新增 `CharFilter`/`Tokenizer`/`TokenFilter` 三个扩展点与对应注册项，`NewChainAnalyzer(cfs, tk, tfs)` 组合 helper（依次应用 char filter → tokenizer → token filter）。② 内置组件：`standard`/`keyword`/`whitespace` 三个切词器（转小写从切词器拆出，`analysis` 包保留为不依赖 plugin 的纯实现库）；`lowercase`/`stop`（Lucene 默认英语停用词表，删除停用词保留 Position 跳号）词元过滤器；`mapping` 字符过滤器样板。组合分词器：`standard`（standard+lowercase，与历史行为一致）、`keyword`、`whitespace`（whitespace+lowercase）、`stop`（standard+lowercase+stop）。③ 引擎接线：`segment.Doc.Terms` 改 `map[string][]plugin.Token`，写段与内存缓冲 positions 取 `Token.Position`（不再用下标，token filter 跳号得以保留）；match 用 `Analyze` 产出的 Term；match_phrase 按各 term 相对首个 term 的位置偏移对齐（索引/查询两侧走同一分析链，偏移同构），停用词跳号时短语语义与 ES 一致。④ 契约更新：RunAnalyzerSuite 新增"Position 单调不减"断言，新增 RunTokenizerSuite/RunTokenFilterSuite/RunCharFilterSuite，内置插件与 examples/custom-plugin（revtext 改为切词器+词元过滤器组合的样板）均跑对应套件。
- 验证：stop 跳号核心用例（"the quick fox" 索引后 quick@1/fox@2，"quick fox" 命中、"quick the fox" 只命中同样含跳号位置的文档）、mapping char filter 引擎级生效、standard 链与升级前 term 序列逐项回归（`index/analysis_chain_test.go`、`plugins/analyzer/analyzer_test.go`）。
- **已完成（2026-10-05，查询插件部分）**：① match_phrase 解析器注册（执行器此前已落地）：简写/完整形态，slop>0 报"暂未支持"。② multi_match：`MatchNode` 加 `Boost`（eval 时 score *= boost），`fields:["title^3",...]` 展开为 `BoolNode{Should}`——should 并集加分近似 best_fields（ES 为 dis_max 取最高分，此处分数累加；type 参数不支持）。③ prefix/wildcard/fuzzy：词典扫描 API 先行（`dict.Reader.Scan/ScanPrefix` 借稀疏索引定位起始块；`posting.FieldReader.Terms` 带截断上限；缓冲侧直接遍历内存 term map），`ParseContext.ExpandTerms` 钩子由引擎注入（自持读锁，协调层纯 schema 解析时留空、分片侧重新解析才展开，与 ES per-shard rewrite 同构），产出 `MultiTermNode`（并集、恒定 1 分、filter 语义）；wildcard 首字符非通配走字面前缀缩范围否则全词典扫描，prefix/wildcard 上限 1024；fuzzy 内联 Levenshtein（≤2，对齐 ES 上限），fuzziness 默认 2、max_expansions 默认 50；展开去重排序后截断，不承诺大词典性能。④ exists：`ExistsNode`，缓冲侧查 `segment.Doc.HasField`（与段 has 位同语义）；段侧 v2 走全字段 has 位，v1 段/stored 类字段/后加字段的老段回落原文 JSON 检查（慢但语义一致）；空串 text 0 token、null 均视为不存在（对齐 ES）。
- 验证：`plugins/query` 契约与形态用例（含 wildcard/Levenshtein 单测、距离 1/2/3 边界）、`index/query_plugins_test.go` 引擎级用例（phrase DSL 端到端、boost 影响排序、prefix/wildcard 横跨缓冲与段、1024 截断、fuzzy 距离边界、exists 缺失/null/空串/不可分词语义、v1 段 exists 回落）。

### P1-7 schema/mapping 体系升级
- ~~现状：动态 mapping 简陋（string→text、数字→number 一刀切，`index/engine.go:235-281`）。~~
- 目标：支持 multi-fields（一个字段同时 text+keyword）、日期自动检测、嵌套对象、mapping 显式更新。
- **已完成（2026-10-05）**：① Field 结构升级（`Analyzer` 字段级覆盖 + `Fields` multi-fields 子字段，一层为止；`Schema.add` 展开为点路径注册，byName 以点路径为键；显式声明字段名禁止含 `.`，先行校验避免与对象展开歧义）。② 嵌套对象展开（`index.flattenDoc`：{"user":{"name":"x"}} → user.name，写入/回放/merge 解析入口统一；数组不展开保持现状）。③ 动态推断重构到 `schema/infer.go` 的 `InferField`：数字→number、bool→bool、字符串→（`date_detection` 开启且日期解析成功）date、否则 text+keyword 子字段（对齐 ES）；`date_detection` 进 IndexSettings/index.json（*bool，nil 默认 true）并随 CSM IndexMeta 分发；日期检测器由 schema 包定义注册点、plugins/fieldtype init 注入（避免循环依赖）。④ mapping 显式更新：`Schema.MergeFields`（新字段含已有字段的新子字段追加、Type/Analyzer 冲突报错、完全一致幂等、预检通过才应用）→ Engine/Index.UpdateMapping（persistSchemaLocked 复用 tmp+rename）→ REST `PUT/GET /{index}/_mapping`（集群模式经 ClusterProvider.UpdateMapping 走 leader 提案）。⑤ 集群广播：`CmdUpdateMapping` + `Command.Mapping` 增量载荷，CSM apply 用纯结构只增合并（`schema.MergeMappingJSON`，不依赖插件注册表保证 FSM 确定性）；master 经 raft apply 回调、data-only 经轮询对账把 CSM mapping 合并进本地分片（`node/mapping.go` applyLocalMapping，冲突时降级逐字段尽力合并）；primary 动态推断出新字段后经新 RPC `UpdateMapping` 异步提案（本地并集 vs CSM diff 出增量，按字段数标记抑制重复广播）。
- 验证：`schema/schema_test.go`（multi-fields 展开、含 `.` 拒绝、子字段禁嵌套、analyzer 覆盖校验、推断规则含日期开关、MergeFields/DiffFields/MergeMappingJSON）、`index/engine_test.go`（嵌套对象查询、title.keyword 整串 term、日期检测开关、analyzer 覆盖、UpdateMapping 持久化）、`api/server_test.go`（PUT 新字段可查/改类型 400/幂等 200/GET mapping/集群路由）、`node/cluster_integration_test.go` TestClusterMappingBroadcast（显式与动态推断的 mapping 变更经 raft 广播到各节点生效）。
- 遗留限制（README 同步）：mapping 广播窗口期——primary 推断 → raft apply/轮询到达之间，未收到含新字段文档的分片暂时没有该字段（收到文档的分片自行推断可立即检索），CSM 到达后新写入正常；老段没有新字段索引文件视为字段缺失，merge 后自愈；同名字段在不同分片被推断为不同类型时先到者入 CSM，其余分片保留本地定义（类型分歧）。

### P1-8 docvalues 类型补齐
- 事实修正：date/bool **已经**复用 number 定长列（`plugins/fieldtype/fieldtype.go` + `segment/writer.go`，date=unix 秒精度到秒、bool=1/0），无列存缺口。真正缺口：keyword ord 列（`docvalues/keyword.go`）已实现但未接入段格式；text 排序/聚合缺引导性报错；协调层多分片 date 排序归并有 bug（从 `_source` 猜测排序值，date 字符串被当缺失排最后）。
- **已完成（2026-10-05，段格式 v2 一部分）**：keyword ord 列已接入段格式——`FieldTypePlugin` 接口以 `DocValuesKind() DVKind`（DVNone/DVNum/DVKeyword）取代 `DocValues() bool`（破坏性变更，无兼容期）；段新增 `kw.<field>` 文件与 meta.KwFields，reader 暴露 `Kw(field)`；引擎写入侧填 `Doc.Kws`，缓冲加 kw 访问器。同批 v2 改动：`has.<field>` 扩到全部字段（exists 查询依赖）。
- **已完成（2026-10-05，阶段 9 收尾）**：① keyword dv 字段排序落地——`validateSortLocked` 放行 DVKeyword，`sortCandidatesLocked` 新增 term 字符串比较路径（kw 列 ord 是每段局部的字典序号，跨段只能比 term 字符串本身；缓冲走 `buf.kw`，缺失恒排最后）；v1 老段无 kw 列且排序器无法回落原文，报带 `_flush`/merge 指引的错误（同 match_phrase 的 v1 策略），merge 升级 v2 后自愈。② 字符串聚合按字段分派——DVKeyword 字段走 kw 列/Kws（免原文 JSON 解析），段无 kw 列（v1 老段、字段后加的老段）回落 stored 原文解析（语义不变），空串两条路径都视为缺失。③ text 本体排序/聚合返回引导性报错（"请使用其 keyword 子字段（如 title.keyword）"），**明确不做 fielddata**；排序校验同时挂在引擎 `validateSortLocked` 与协调层 `NewScatterPlan`（后者快速失败，避免分片错误被 node 降级语义吞成 partial 空结果），聚合报错在 `plugins/agg` 解析期（协调层同样可见）。④ 协调层排序归并修复——`Hit` 新增 `Sort []json.RawMessage`（分片侧填实际比较键：number/date/bool→int64、keyword→string、_score→float、缺失→null），`ScatterPlan.Merge` 按位比较（字符串字典序/数字按数值），废弃 `hitNumValue` 的 `_source` 猜测（date 字符串被当缺失的 bug 修复）；`Sort` 字段经 transport JSON 跨节点传输无碍（测试锁定）。
- 验证：`index/kernel_test.go`（keyword/子字段排序、text 引导报错、flush 前后 sort+terms/avg+range 对拍）、`index/shard_test.go` TestMultiShardSortMerge（多分片 date/keyword 排序归并+分页+排序键 JSON 往返，修复前已验证该用例失败）、`index/phrase_test.go`（v1 段 keyword 排序报错、merge 后自愈、v1 段聚合回落与纯 v2 对拍）、`plugins/agg`（text 聚合报错）。

## P2：性能、稳定性与运维

- **P2-9 segment 合并策略**：现状是"<10000 doc 小段超 5 个就合"的朴素策略（`index/engine.go:470-544`），对标 Lucene tiered merge policy，加 force merge 与合并并发控制。
- **P2-10 缓存与熔断**：无 query cache / request cache / fielddata 管理，无 circuit breaker 与内存 buffer 限流——ES 生产稳定性的半壁江山。
- **P2-11 transport 安全**：gRPC 无 TLS、REST 无鉴权，生产不可用。
- **P2-12 现代检索能力**：对标 ES 8.x 需 `dense_vector` 字段 + kNN 向量检索（可复用插件体系新增 fieldtype + query）。

## P3：生态与高级功能

- **P3-13 聚合扩展**：date_histogram、histogram、percentiles、pipeline aggs（两段式框架已有，纯增量）。
- **P3-14 快照/恢复**（snapshot/restore）与索引生命周期管理。
- **P3-15 API 兼容性**：bulk 部分失败语义、`_mget/_msearch`、aliases、index templates——决定 ES 用户能否平滑迁移。

## 执行建议

- P0 的 1–4 作为一个整体先做（共同构成"集群数据不丢不错"的底线，且相互耦合）。
- P1 里 positions（P1-5）已完成（段格式 v2，2026-10-05）。
- P2/P3 按实际使用场景挑选。
