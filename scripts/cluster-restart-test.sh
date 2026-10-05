#!/usr/bin/env bash
# falcon 集群全停全启回归（P0-1 raft 元数据持久化）：
# 3 节点建索引写 20 篇 → 全部停止 → 同目录全部重启 ——
# 验证元数据无需重注册即在（路由表一致）、已 ack 文档可查（total 不变）、
# 重启后继续写成功并复制。
#
# 运行：make restart-test  或  bash scripts/cluster-restart-test.sh
set -euo pipefail

cd "$(dirname "$0")/.."

BASE_PORT="${FALCON_RESTART_PORT:-19300}"
WORK="$(mktemp -d /tmp/falcon-restart.XXXXXX)"
BIN=bin/falcon
PIDS=()

cleanup() {
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
    rm -rf "$WORK"
}
trap cleanup EXIT

say() { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }

wait_http() {
    for _ in $(seq 1 100); do
        if curl -s "http://127.0.0.1:$1/_cluster/health" >/dev/null 2>&1; then return 0; fi
        sleep 0.2
    done
    echo "节点 $1 启动超时" >&2; exit 1
}

http_port() { echo $((BASE_PORT + ($1 - 1) * 10)); }

say "构建"
make build

# 节点配置：node1 自举（无 seeds），node2/node3 以 node1 为 seed
for i in 1 2 3; do
    HTTP_PORT=$(http_port $i)
    GRPC_PORT=$((HTTP_PORT + 1))
    mkdir -p "$WORK/n$i"
    cat > "$WORK/n$i/config.yaml" <<EOF
node:
  name: node$i
  master: true
  data: true
http:
  port: ${HTTP_PORT}
grpc:
  port: ${GRPC_PORT}
data:
  path: ${WORK}/n$i/data
cluster:
  name: falcon-restart-test
EOF
    if [ "$i" -gt 1 ]; then
        cat >> "$WORK/n$i/config.yaml" <<EOF
  seeds:
    - 127.0.0.1:$((BASE_PORT + 1))
EOF
    fi
done

start_all() {
    for i in 1 2 3; do
        "$BIN" --config "$WORK/n$i/config.yaml" >> "$WORK/n$i/node.log" 2>&1 &
        PIDS+=($!)
        wait_http "$(http_port $i)"
    done
}

stop_all() {
    say "停止全部节点"
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && wait "$p" 2>/dev/null || true; done
    PIDS=()
}

say "启动 3 节点（node1 自举，node2/node3 经 seed 加入）"
start_all

say "等待三节点成员表收敛"
for i in 1 2 3; do
    HTTP_PORT=$(http_port $i)
    for _ in $(seq 1 100); do
        N=$(curl -s "http://127.0.0.1:${HTTP_PORT}/_cluster/state" | jq '.nodes | length')
        [ "$N" = "3" ] && break
        sleep 0.3
    done
    [ "$N" = "3" ] || { echo "node$i 成员表未收敛: $N" >&2; exit 1; }
done

HTTP1=$(http_port 1)
say "建索引 logs：3 分片 1 副本，wait_for_active_shards=all"
curl -s -X PUT "http://127.0.0.1:${HTTP1}/logs" -d '{"settings":{"number_of_shards":3,"number_of_replicas":1,"write_wait_for_active_shards":"all"},"mappings":{"fields":[{"name":"content","type":"text"},{"name":"level","type":"number"}]}}' | jq -c .

say "写入 20 篇文档"
for i in $(seq 1 20); do
    printf '{"content":"重启测试 文档 %s","level":%s}' "$i" "$i" | \
        curl -s -o /dev/null -X PUT "http://127.0.0.1:${HTTP1}/logs/_doc/$i" --data-binary @-
done

# 等复制追平（心跳 1s 上报一次，给足余量）
sleep 3
ROUTES_BEFORE=$(curl -s "http://127.0.0.1:${HTTP1}/_cluster/state" | jq -cS '.routes.logs')
TOTAL_BEFORE=$(curl -s -X POST "http://127.0.0.1:$(http_port 2)/logs/_search" -d '{"query":{"match_all":{}},"size":50}' | jq -r '.total')
echo "重启前路由表: $ROUTES_BEFORE"
echo "重启前 total: $TOTAL_BEFORE"
[ "$TOTAL_BEFORE" = "20" ] || { echo "重启前查询失败 total=$TOTAL_BEFORE" >&2; exit 1; }

stop_all

say "同目录重启全部节点（raft 元数据从磁盘恢复，不重注册）"
start_all

say "校验：路由表一致 + total 不变"
for i in 1 2 3; do
    HTTP_PORT=$(http_port $i)
    # 元数据恢复等待（选主 + raft 重放）
    for _ in $(seq 1 100); do
        R=$(curl -s "http://127.0.0.1:${HTTP_PORT}/_cluster/state" | jq -cS '.routes.logs // empty')
        [ -n "$R" ] && break
        sleep 0.3
    done
    [ "$R" = "$ROUTES_BEFORE" ] || { echo "node$i 路由表不一致: $R vs $ROUTES_BEFORE" >&2; exit 1; }
    echo "node$i 路由表一致: $R"
done
for i in 1 2 3; do
    HTTP_PORT=$(http_port $i)
    for _ in $(seq 1 100); do
        T=$(curl -s -X POST "http://127.0.0.1:${HTTP_PORT}/logs/_search" -d '{"query":{"match_all":{}},"size":50}' | jq -r '.total' 2>/dev/null || true)
        [ "$T" = "20" ] && break
        sleep 0.3
    done
    [ "$T" = "20" ] || { echo "node$i 重启后 total=$T, want 20" >&2; exit 1; }
    echo "node$i 重启后 total=$T"
done

say "重启后继续写入并验证复制"
for i in $(seq 21 26); do
    printf '{"content":"重启后 文档 %s","level":%s}' "$i" "$i" | \
        curl -s -o /dev/null -X PUT "http://127.0.0.1:${HTTP1}/logs/_doc/$i" --data-binary @-
done
sleep 3
for i in 2 3; do
    HTTP_PORT=$(http_port $i)
    for _ in $(seq 1 100); do
        T=$(curl -s -X POST "http://127.0.0.1:${HTTP_PORT}/logs/_search" -d '{"query":{"match_all":{}},"size":50}' | jq -r '.total' 2>/dev/null || true)
        [ "$T" = "26" ] && break
        sleep 0.3
    done
    [ "$T" = "26" ] || { echo "node$i 继续写后 total=$T, want 26" >&2; exit 1; }
    echo "node$i 继续写后 total=$T"
done

say "清理并退出（cluster restart test 全部通过）"
