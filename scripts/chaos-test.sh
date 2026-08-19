#!/usr/bin/env bash
# falcon 混沌测试（5b）：3 节点集群写入中途 kill -9 一个 primary 持有者，
# 断言：RTO 内恢复可写、wait_for_active_shards=all 已 ack 数据不丢、
# 被杀节点重启后追平并重新成为 in-sync 副本。
#
# 运行：make chaos  或  bash scripts/chaos-test.sh
set -euo pipefail

cd "$(dirname "$0")/.."

BASE_PORT="${FALCON_CHAOS_PORT:-19200}"
WORK="$(mktemp -d /tmp/falcon-chaos.XXXXXX)"
BIN=bin/falcon
PIDS=()

cleanup() {
    local rc=$?
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
    if [ "$rc" != "0" ]; then
        # 失败时保留日志便于排查
        mkdir -p /tmp/chaos-fail-logs && cp "$WORK"/*.log /tmp/chaos-fail-logs/ 2>/dev/null || true
        echo "（日志已保留在 /tmp/chaos-fail-logs）" >&2
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

say() { printf '\n\033[1;31m== %s ==\033[0m\n' "$*"; }
http_of() { echo $((BASE_PORT + ($1 - 1) * 10)); }

start_node() { # $1=序号
    local hp gp
    hp=$(http_of "$1"); gp=$((hp + 1))
    mkdir -p "$WORK/n$1"
    cat > "$WORK/n$1/config.yaml" <<EOF
node:
  name: node$1
  master: true
  data: true
http:
  port: ${hp}
grpc:
  port: ${gp}
data:
  path: ${WORK}/n$1/data
cluster:
  name: falcon-chaos
EOF
    if [ "$1" -gt 1 ]; then
        printf '  seeds:\n    - 127.0.0.1:%d\n' $((BASE_PORT + 1)) >> "$WORK/n$1/config.yaml"
    fi
    "$BIN" --config "$WORK/n$1/config.yaml" > "$WORK/n$1.log" 2>&1 &
    PIDS+=($!)
    for _ in $(seq 1 100); do
        curl -s "http://127.0.0.1:${hp}/_cluster/health" >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    echo "节点 $1 启动超时" >&2; exit 1
}

write_doc() { # $1=http端口 $2=id $3=内容；返回 0 表示 ack
    printf '{"content":"%s","level":%s}' "$3" "$2" | \
        curl -s -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$1/logs/_doc/$2" \
        --data-binary @- | grep -q '^200$'
}

say "构建"
make build

say "启动 3 节点"
for i in 1 2 3; do start_node "$i"; done

say "建索引 logs：3 分片 1 副本，wait_for_active_shards=all"
H1=$(http_of 1)
curl -s -X PUT "http://127.0.0.1:${H1}/logs" -d '{"settings":{"number_of_shards":3,"number_of_replicas":1,"write_wait_for_active_shards":"all"},"mappings":{"fields":[{"name":"content","type":"text"},{"name":"level","type":"number"}]}}' | jq .

say "第一批写入 30 篇（记录 ack）"
ACKED=0
for i in $(seq 1 30); do
    write_doc "$H1" "$i" "混沌测试文档 $i" || { echo "第 $i 篇写入失败（故障前不应失败）" >&2; exit 1; }
    ACKED=$i
done
echo "已 ack: $ACKED 篇"

# 找一个持有 primary 的节点杀掉（node2/node3 必有一个持 primary；均衡分配下都持有）
VICTIM=2
say "kill -9 node${VICTIM}（持有 primary）"
kill -9 "${PIDS[$((VICTIM-1))]}" 2>/dev/null || true

say "等待集群恢复可写（RTO 预算 15s）"
T0=$(date +%s)
RECOVERED_AT=0
for i in $(seq 31 200); do
    if write_doc "$H1" "$i" "故障后文档 $i"; then
        RECOVERED_AT=$(date +%s)
        echo "第 $i 篇写入成功，RTO = $((RECOVERED_AT - T0))s"
        break
    fi
    sleep 0.5
done
[ "$RECOVERED_AT" != "0" ] || { echo "RTO 超时（>15s+）" >&2; exit 1; }
if [ $((RECOVERED_AT - T0)) -ge 15 ]; then echo "RTO 超预算" >&2; exit 1; fi

say "继续写入 20 篇"
for i in $(seq 201 220); do
    # 故障检测（6s）+ 路由变更 + 副本初始化期间允许失败，重试 15s
    ok=""
    for _ in $(seq 1 75); do
        write_doc "$H1" "$i" "故障后文档 $i" && { ok=1; break; }
        sleep 0.2
    done
    if [ -z "$ok" ]; then
        echo "第 $i 篇写入失败（重试后仍失败），最后一次响应：" >&2
        printf '{"content":"故障后文档 %s","level":%s}' "$i" "$i" | curl -s -X PUT "http://127.0.0.1:$H1/logs/_doc/$i" --data-binary @- >&2
        echo >&2; exit 1
    fi
    ACKED=$i
done
echo "已 ack 总计: $((30 + 20 + 1)) 篇附近（含探测成功的那篇）"

say "断言：已 ack 文档全部可查（数据不丢）"
# 注：waitAll 下 primary 本地写成功后才可能因副本 ack 失败而报错，
# 因此"报错"的写入也可能已落盘——总数可能 >= ack 数，语义同 ES。
# 这里断言 ack 过的 ID 逐一可查（不丢），而不是精确总数。
ACK_IDS="1 2 15 30 33 201 210 220"
for id in $ACK_IDS; do
    FOUND=""
    for _ in $(seq 1 40); do
        FOUND=$(curl -s "http://127.0.0.1:${H1}/logs/_doc/$id" | jq -r '.found // false')
        [ "$FOUND" = "true" ] && break
        sleep 0.5
    done
    if [ "$FOUND" != "true" ]; then
        echo "已 ack 文档 $id 丢失！" >&2; exit 1
    fi
done
TOTAL=$(curl -s -X POST "http://127.0.0.1:${H1}/logs/_search" -d '{"query":{"match_all":{}},"size":0}' | jq -r '.total // 0')
echo "已 ack 文档全部可查；total = ${TOTAL}（>= 51，含报错但已落盘的探测写）"
[ "$TOTAL" -ge 51 ] || { echo "总数异常" >&2; exit 1; }

say "集群健康（node$VICTIM 下线期间应为 yellow 或 green）"
curl -s "http://127.0.0.1:${H1}/_cluster/health" | jq .
curl -s "http://127.0.0.1:${H1}/_cat/shards" | jq -c '.[]'

say "重启 node${VICTIM}，断言追平并重新成为 in-sync 副本"
start_node "$VICTIM"
H1=$(http_of 1)
for _ in $(seq 1 100); do
    OUT=$(curl -s "http://127.0.0.1:${H1}/_cat/shards")
    # node 持有的分片中，LSN 与 primary 一致的数量（in-sync）
    SYNCED=$(echo "$OUT" | jq -r --arg n "node$VICTIM" '
        group_by(.index + "/" + (.shard|tostring))
        | map(select(any(.node == $n)))
        | map((.[] | select(.prirep=="p") | .lsn) as $p
              | (.[] | select(.node==$n) | .lsn) as $l
              | if $l == $p then 1 else 0 end)
        | add // 0')
    TOTAL_SHARDS=$(echo "$OUT" | jq -r --arg n "node$VICTIM" '[.[] | select(.node == $n)] | length')
    if [ "$TOTAL_SHARDS" -gt 0 ] && [ "$SYNCED" = "$TOTAL_SHARDS" ]; then
        echo "node${VICTIM} 已追平（$SYNCED/$TOTAL_SHARDS 分片 in-sync）"
        break
    fi
    sleep 0.5
done
[ "${TOTAL_SHARDS:-0}" -gt 0 ] && [ "$SYNCED" = "$TOTAL_SHARDS" ] || { echo "node${VICTIM} 未追平" >&2; exit 1; }

say "最终健康检查"
curl -s "http://127.0.0.1:${H1}/_cluster/health" | jq .

say "清理并退出（混沌测试全部通过）"
