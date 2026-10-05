#!/usr/bin/env bash
# falcon 单机端到端演示：
#   启动节点 → 建索引 → bulk 灌入中文微博样例 → match/range/bool/聚合查询
#   → 删除验证 → kill -9 后重启验证 translog 崩溃恢复 → 清理
#
# 运行：make demo  或  bash scripts/demo.sh
set -euo pipefail

cd "$(dirname "$0")/.."

PORT="${FALCON_DEMO_PORT:-19399}"
DATA_DIR="$(mktemp -d /tmp/falcon-demo.XXXXXX)"
BASE="http://127.0.0.1:${PORT}"
BIN=bin/falcon
PID=""

cleanup() {
    [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

say() { printf '\n\033[1;34m== %s ==\033[0m\n' "$*"; }

wait_ready() {
    for _ in $(seq 1 50); do
        if curl -s "${BASE}/_cluster/health" >/dev/null 2>&1; then return 0; fi
        sleep 0.2
    done
    echo "节点启动超时" >&2; exit 1
}

say "构建"
make build

say "启动节点（端口 ${PORT}，数据目录 ${DATA_DIR}）"
cat > "${DATA_DIR}/config.yaml" <<EOF
http:
  port: ${PORT}
data:
  path: ${DATA_DIR}/data
EOF
"$BIN" --config "${DATA_DIR}/config.yaml" >/dev/null 2>&1 &
PID=$!
wait_ready
curl -s "${BASE}/_cluster/health" | jq .

say "创建索引 weibo（datetime=date / name,level=keyword / content=text / likes=number）"
curl -s -X PUT "${BASE}/weibo" -d '{"mappings":{"fields":[
  {"name":"datetime","type":"date"},
  {"name":"name","type":"keyword"},
  {"name":"level","type":"keyword"},
  {"name":"content","type":"text"},
  {"name":"likes","type":"number"}]}}' | jq .

say "bulk 灌入 24 条中文微博"
{
  names=("延参法师" "梦想家林志颖" "尐笨蛋晴空" "拐栋六R" "爱尔威智能" "小米官方")
  levels=("黄V" "蓝V" "普通用户" "达人")
  contents=("看山东，赞山东，和大家一起拉呱" "雅礼中学开始报名了" "可以回雅礼试试" "转发微博" "加油!!!" "嗯，看时间，潍坊见哦" "今天天气不错，适合爬山" "新手机发布会直播进行中")
  for i in $(seq 1 24); do
    n=$((i % 6)); l=$((i % 4)); c=$((i % 8))
    day=$(printf "%02d" $((i % 28 + 1)))
    likes=$((i * 37 % 500))
    printf '{"index":{"_id":"%d"}}\n' "$i"
    printf '{"datetime":"2015-11-%s 12:00:00","name":"%s","level":"%s","content":"%s","likes":%d}\n' \
      "$day" "${names[$n]}" "${levels[$l]}" "${contents[$c]}" "$likes"
  done
} > "${DATA_DIR}/bulk.ndjson"
curl -s -X POST "${BASE}/weibo/_bulk" --data-binary @"${DATA_DIR}/bulk.ndjson" | jq '{errors, count: (.items | length)}'

say "查询 1：match 全文检索 content=雅礼"
curl -s -X POST "${BASE}/weibo/_search" -d '{"query":{"match":{"content":"雅礼"}},"size":3}' \
  | jq '{total, hits: [.hits[] | {id, score, name: ._source.name, content: ._source.content}]}'

say "查询 2：range 日期过滤（2015-11-10 ~ 2015-11-20）+ likes 排序"
curl -s -X POST "${BASE}/weibo/_search" -d '{"query":{"range":{"datetime":{"gte":"2015-11-10","lte":"2015-11-20 23:59:59"}}},"sort":[{"likes":"desc"}],"size":3}' \
  | jq '{total, hits: [.hits[] | {id, likes: ._source.likes, datetime: ._source.datetime}]}'

say "查询 3：bool（must 含雅礼 AND filter 等级=普通用户）"
curl -s -X POST "${BASE}/weibo/_search" -d '{"query":{"bool":{"must":[{"match":{"content":"雅礼"}}],"filter":[{"term":{"level":"普通用户"}}]}}}' \
  | jq '{total, hits: [.hits[] | {id, name: ._source.name, level: ._source.level}]}'

say "查询 4：terms 聚合（各等级微博数）+ avg likes"
curl -s -X POST "${BASE}/weibo/_search" -d '{"query":{"match_all":{}},"size":0,"aggs":{"by_level":{"terms":{"field":"level"}},"avg_likes":{"avg":{"field":"likes"}},"levels":{"cardinality":{"field":"level"}}}}' \
  | jq '.aggs'

say "删除文档 1 并验证"
curl -s -X DELETE "${BASE}/weibo/_doc/1" | jq .
curl -s "${BASE}/weibo/_doc/1" | jq -c .

say "flush（缓冲落盘为段；refresh 语义已轻量化，落盘走 _flush）"
curl -s -X POST "${BASE}/weibo/_flush" | jq .

TOTAL_BEFORE=$(curl -s -X POST "${BASE}/weibo/_search" -d '{}' | jq .total)

say "kill -9 模拟崩溃（文档 999 不 flush，仅靠 translog 恢复）"
curl -s -X PUT "${BASE}/weibo/_doc/999" -d '{"datetime":"2015-12-01 00:00:00","name":"崩溃恢复测试","level":"达人","content":"这条数据只进了 translog","likes":1}' > /dev/null
kill -9 "$PID"
PID=""
sleep 0.5

say "重启节点，验证 translog 崩溃恢复"
"$BIN" --config "${DATA_DIR}/config.yaml" >/dev/null 2>&1 &
PID=$!
wait_ready
# 重启后 raft CSM 重建 + 本地索引重注册需要一点时间，轮询等待
RECOVERED=""
for _ in $(seq 1 50); do
    RECOVERED=$(curl -s "${BASE}/weibo/_doc/999" | jq -r '._source.name // "未恢复"')
    [ "$RECOVERED" = "崩溃恢复测试" ] && break
    sleep 0.2
done
TOTAL_AFTER=$(curl -s -X POST "${BASE}/weibo/_search" -d '{}' | jq .total)
echo "崩溃前 total=${TOTAL_BEFORE}，重启后 total=${TOTAL_AFTER}（+1 为未落盘的文档 999）"
echo "文档 999 name=${RECOVERED}"
curl -s "${BASE}/_cat/indices" | jq .

if [ "$RECOVERED" != "崩溃恢复测试" ]; then
  echo "崩溃恢复验证失败" >&2; exit 1
fi

say "清理并退出（demo 全部通过）"
