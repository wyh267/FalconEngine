#!/usr/bin/env bash
# falcon 集群演示（5a）：一键起 3 个 master+data 节点，
# 验证节点加入、raft 元数据复制、分片路由表一致性与本地分片落地。
#
# 运行：make cluster-demo  或  bash scripts/cluster-demo.sh
set -euo pipefail

cd "$(dirname "$0")/.."

BASE_PORT="${FALCON_CLUSTER_PORT:-19100}"
WORK="$(mktemp -d /tmp/falcon-cluster.XXXXXX)"
BIN=bin/falcon
PIDS=()

cleanup() {
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
    rm -rf "$WORK"
}
trap cleanup EXIT

say() { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }

wait_http() {
    for _ in $(seq 1 50); do
        if curl -s "http://127.0.0.1:$1/_cluster/health" >/dev/null 2>&1; then return 0; fi
        sleep 0.2
    done
    echo "节点 $1 启动超时" >&2; exit 1
}

say "构建"
make build

# 节点配置：node1 自举（无 seeds），node2/node3 以 node1 为 seed
for i in 1 2 3; do
    HTTP_PORT=$((BASE_PORT + (i - 1) * 10))
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
  name: falcon-demo
EOF
    if [ "$i" -gt 1 ]; then
        cat >> "$WORK/n$i/config.yaml" <<EOF
  seeds:
    - 127.0.0.1:$((BASE_PORT + 1))
EOF
    fi
done

say "启动 3 节点（node1 自举，node2/node3 经 seed 加入）"
for i in 1 2 3; do
    HTTP_PORT=$((BASE_PORT + (i - 1) * 10))
    "$BIN" --config "$WORK/n$i/config.yaml" > "$WORK/n$i/node.log" 2>&1 &
    PIDS+=($!)
    wait_http "$HTTP_PORT"
done

say "等待三节点成员表收敛"
for i in 1 2 3; do
    HTTP_PORT=$((BASE_PORT + (i - 1) * 10))
    for _ in $(seq 1 100); do
        N=$(curl -s "http://127.0.0.1:${HTTP_PORT}/_cluster/state" | jq '.nodes | length')
        [ "$N" = "3" ] && break
        sleep 0.3
    done
    N=$(curl -s "http://127.0.0.1:${HTTP_PORT}/_cluster/state" | jq '.nodes | length')
    echo "node$i 视角节点数: $N"
    [ "$N" = "3" ] || { echo "成员表未收敛" >&2; exit 1; }
done

say "在 node1（自举 leader）建索引 logs：3 分片 1 副本"
HTTP1=$((BASE_PORT))
curl -s -X PUT "http://127.0.0.1:${HTTP1}/logs" -d '{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"fields":[{"name":"content","type":"text"},{"name":"level","type":"number"}]}}' | jq .

say "等待路由表复制到全部节点并校验一致"
R0=""
for i in 1 2 3; do
    HTTP_PORT=$((BASE_PORT + (i - 1) * 10))
    for _ in $(seq 1 100); do
        R=$(curl -s "http://127.0.0.1:${HTTP_PORT}/_cluster/state" | jq -c '.routes.logs // empty')
        [ -n "$R" ] && break
        sleep 0.3
    done
    echo "node$i 路由表: $R"
    if [ -z "$R0" ]; then R0="$R"; elif [ "$R" != "$R0" ]; then
        echo "路由表不一致" >&2; exit 1
    fi
done

say "验证各节点本地分片落地（3 分片 × (1主+1副) = 6 片，3 节点各持 2 片）"
sleep 1  # 等 apply 回调建完分片
for i in 1 2 3; do
    HTTP_PORT=$((BASE_PORT + (i - 1) * 10))
    curl -s "http://127.0.0.1:${HTTP_PORT}/_cat/indices" | jq -c '.[] | select(.name=="logs")'
done

say "写入数据并跨节点查询（写入 node1，查询 node2/node3 验证 scatter-gather）"
for i in 1 2 3 4 5 6; do
    printf '{"content":"分布式 集群 文档 %s","level":%s}' "$i" "$i" | \
        curl -s -o /dev/null -X PUT "http://127.0.0.1:${HTTP1}/logs/_doc/$i" --data-binary @-
done
sleep 2  # 等复制与心跳
for i in 2 3; do
    HTTP_PORT=$((BASE_PORT + (i - 1) * 10))
    TOTAL=$(curl -s -X POST "http://127.0.0.1:${HTTP_PORT}/logs/_search" -d '{"query":{"match":{"content":"分布式"}},"size":10}' | jq -r '.total')
    echo "node$i 查询 total=$TOTAL"
    [ "$TOTAL" = "6" ] || { echo "跨节点查询失败" >&2; exit 1; }
done
# 聚合跨节点合并
curl -s -X POST "http://127.0.0.1:${HTTP1}/logs/_search" -d '{"size":0,"aggs":{"sum_level":{"sum":{"field":"level"}}}}' | jq -c '.aggs'

say "集群健康"
curl -s "http://127.0.0.1:${HTTP1}/_cluster/health" | jq .

say "删除索引 logs（经 leader node1，验证集群级路由清理）"
curl -s -X DELETE "http://127.0.0.1:${HTTP1}/logs" | jq .
for i in 1 2 3; do
    HTTP_PORT=$((BASE_PORT + (i - 1) * 10))
    for _ in $(seq 1 100); do
        R=$(curl -s "http://127.0.0.1:${HTTP_PORT}/_cluster/state" | jq -c '.routes.logs // empty')
        [ -z "$R" ] && [ ! -d "$WORK/n$i/data/logs" ] && break
        sleep 0.3
    done
    [ -z "$R" ] || { echo "node$i 路由表未清除" >&2; exit 1; }
    [ ! -d "$WORK/n$i/data/logs" ] || { echo "node$i 数据目录未删除" >&2; exit 1; }
    N=$(curl -s "http://127.0.0.1:${HTTP_PORT}/_cat/indices" | jq '[.[] | select(.name=="logs")] | length')
    [ "$N" = "0" ] || { echo "node$i 本地索引未清理" >&2; exit 1; }
    echo "node$i 索引已删除（路由表/数据目录/本地索引均清除）"
done

say "重建同名索引并验证可写可查"
curl -s -X PUT "http://127.0.0.1:${HTTP1}/logs" -d '{"settings":{"number_of_shards":1,"number_of_replicas":2},"mappings":{"fields":[{"name":"content","type":"text"}]}}' | jq -c .
curl -s -X PUT "http://127.0.0.1:${HTTP1}/logs/_doc/rebuild-1" -d '{"content":"重建后的文档"}' | jq -c .
TOTAL=$(curl -s -X POST "http://127.0.0.1:${HTTP1}/logs/_search" -d '{"query":{"match":{"content":"重建"}}}' | jq -r '.total')
[ "$TOTAL" = "1" ] || { echo "重建索引查询失败 total=$TOTAL" >&2; exit 1; }
echo "重建索引查询 total=$TOTAL"

say "清理并退出（cluster demo 全部通过）"
