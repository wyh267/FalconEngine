#!/usr/bin/env bash
# falcon 副本恢复测试（P0-2 peer recovery）：3 节点集群，
# kill -9 一个副本持有者；其死亡期间 primary 持续写入并循环 _flush
# 强制 translog 轮替（保留窗口越过死亡副本位点 → 落后出窗）；
# 重启该节点，断言：发生段拷贝恢复（日志）、分片 LSN 追平、
# 文档逐条一致（段拷贝 + translog 补差两阶段结果正确）。
#
# 运行：make recovery-test  或  bash scripts/recovery-test.sh
set -euo pipefail

cd "$(dirname "$0")/.."

BASE_PORT="${FALCON_RECOVERY_PORT:-19500}"
WORK="$(mktemp -d /tmp/falcon-recovery.XXXXXX)"
BIN=bin/falcon
PIDS=()

cleanup() {
    local rc=$?
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
    if [ "$rc" != "0" ]; then
        # 失败时保留日志便于排查
        mkdir -p /tmp/recovery-fail-logs && cp "$WORK"/*.log /tmp/recovery-fail-logs/ 2>/dev/null || true
        echo "（日志已保留在 /tmp/recovery-fail-logs）" >&2
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
  name: falcon-recovery
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

write_doc() { # $1=http端口 $2=id $3=内容
    printf '{"content":"%s","level":%s}' "$3" "$2" | \
        curl -s -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$1/logs/_doc/$2" \
        --data-binary @- | grep -q '^200$'
}

# 等待某节点持有的全部分片与其 primary LSN 一致；$1=节点名 $2=超时秒数
wait_synced() { # 返回 0 追平 / 1 超时
    local target="$1" deadline=$(( $(date +%s) + $2 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local out synced total
        out=$(curl -s "http://127.0.0.1:$(http_of 1)/_cat/shards")
        synced=$(echo "$out" | jq -r --arg n "$target" '
            group_by(.index + "/" + (.shard|tostring))
            | map(select(any(.node == $n)))
            | map((.[] | select(.prirep=="p") | .lsn) as $p
                  | (.[] | select(.node==$n) | .lsn) as $l
                  | if $l == $p then 1 else 0 end)
            | add // 0')
        total=$(echo "$out" | jq -r --arg n "$target" '[.[] | select(.node == $n)] | length')
        if [ "$total" -gt 0 ] && [ "$synced" = "$total" ]; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

say "构建"
make build

say "启动 3 节点"
for i in 1 2 3; do start_node "$i"; done

H1=$(http_of 1)
H2=$(http_of 2)

say "建索引 logs：3 分片 1 副本（默认 write_wait_for_active_shards=1）"
curl -s -X PUT "http://127.0.0.1:${H1}/logs" -d '{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"fields":[{"name":"content","type":"text"},{"name":"level","type":"number"}]}}' | jq .

say "第一批写入 30 篇并等副本追平"
for i in $(seq 1 30); do
    write_doc "$H1" "$i" "恢复测试文档 $i" || { echo "第 $i 篇写入失败" >&2; exit 1; }
done
wait_synced "node2" 30 || { echo "node2 未追平" >&2; exit 1; }
wait_synced "node3" 30 || { echo "node3 未追平" >&2; exit 1; }
echo "30 篇已在全部副本就位"

VICTIM=3
say "kill -9 node${VICTIM}（副本持有者）"
kill -9 "${PIDS[$((VICTIM-1))]}" 2>/dev/null || true

say "等故障转移完成（无 primary 留在 node${VICTIM}）"
MIGRATED=""
for _ in $(seq 1 90); do
    P_ON_VICTIM=$(curl -s "http://127.0.0.1:${H1}/_cat/shards" | jq -r --arg n "node${VICTIM}" '[.[] | select(.prirep=="p" and .node==$n)] | length')
    [ "$P_ON_VICTIM" = "0" ] && { MIGRATED=1; break; }
    sleep 0.5
done
[ -n "$MIGRATED" ] || { echo "故障转移超时" >&2; exit 1; }
echo "全部 primary 已离开 node${VICTIM}"

say "死亡期间：8 轮 写入 10 篇 + 对全部存活节点 _flush（强制 translog 轮替）"
for round in $(seq 1 8); do
    for i in $(seq $((round * 10 + 1)) $((round * 10 + 10))); do
        # 故障检测/路由变更窗口允许个别失败，小重试
        ok=""
        for _ in $(seq 1 25); do
            write_doc "$H1" "$i" "轮替期文档 $i" && { ok=1; break; }
            sleep 0.2
        done
        [ -n "$ok" ] || { echo "第 $i 篇写入失败" >&2; exit 1; }
    done
    # 两个存活节点都 flush：各自持有的分片（primary 与副本）全部轮替
    curl -s -X POST "http://127.0.0.1:${H1}/logs/_flush" >/dev/null
    curl -s -X POST "http://127.0.0.1:${H2}/logs/_flush" >/dev/null
    # 给存活副本留拉取+ack 时间，primary 的保留窗口才能推进
    sleep 1
    echo "第 $round 轮完成（已写 $((round * 10 + 10)) 篇）"
done

say "重启 node${VICTIM}"
start_node "$VICTIM"

say "断言：node${VICTIM} 发生段拷贝恢复（日志）"
RECOVERED=0
for _ in $(seq 1 60); do
    RECOVERED=$(grep -c "段拷贝恢复完成" "$WORK/n${VICTIM}.log" 2>/dev/null || true)
    RECOVERED=${RECOVERED:-0}
    [ "$RECOVERED" -gt 0 ] && break
    sleep 0.5
done
if [ "$RECOVERED" -eq 0 ]; then
    echo "node${VICTIM} 日志中无段拷贝恢复记录（保留窗口未出窗或恢复未发生）" >&2
    grep -E "恢复|出窗" "$WORK/n${VICTIM}.log" >&2 || true
    exit 1
fi
echo "段拷贝恢复发生 $RECOVERED 次"
grep "段拷贝恢复" "$WORK/n${VICTIM}.log" | head -5

say "断言：node${VICTIM} LSN 追平"
wait_synced "node${VICTIM}" 60 || { echo "node${VICTIM} 未追平" >&2; exit 1; }
echo "node${VICTIM} 已追平"

say "断言：文档逐条一致（node1 vs node${VICTIM}，共 110 篇）"
H3=$(http_of "$VICTIM")
for id in $(seq 1 110); do
    S1=$(curl -s "http://127.0.0.1:${H1}/logs/_doc/$id" | jq -c '{found, src: ._source}')
    S3=$(curl -s "http://127.0.0.1:${H3}/logs/_doc/$id" | jq -c '{found, src: ._source}')
    if [ "$S1" != "$S3" ]; then
        echo "文档 $id 不一致: node1=$S1 node${VICTIM}=$S3" >&2; exit 1
    fi
done
echo "110 篇文档逐条一致"

say "最终健康检查"
curl -s "http://127.0.0.1:${H1}/_cluster/health" | jq .

say "清理并退出（副本恢复测试全部通过）"
