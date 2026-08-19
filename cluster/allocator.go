package cluster

import (
	"fmt"
	"sort"
)

// AllocateRoutes 为新索引分配分片路由（纯函数，便于测试）。
//
// 策略：逐个分片把 primary 分配给当前持有分片数最少的 data 节点，
// 副本依次分配给次少且不等于 primary 的节点；节点数不足时副本数降级。
// 负载计数包含已有路由表，使多次建索引之间也保持均衡。
func AllocateRoutes(sm *StateMachine, numShards, numReplicas int) ([]ShardRoute, error) {
	nodes := sm.DataNodes()
	if len(nodes) == 0 {
		return nil, fmt.Errorf("cluster: 没有可用的 data 节点")
	}
	if numShards <= 0 {
		numShards = 1
	}

	// 统计各节点当前持有的分片数（primary + replica）
	load := map[uint64]int{}
	for _, n := range nodes {
		load[n.ID] = 0
	}
	sm.mu.RLock()
	for _, routes := range sm.Routes {
		for _, r := range routes {
			load[r.Primary]++
			for _, id := range r.Replicas {
				load[id]++
			}
		}
	}
	sm.mu.RUnlock()

	// 按 (负载, ID) 升序取节点，保证确定性
	byLoad := func() []uint64 {
		ids := make([]uint64, 0, len(nodes))
		for _, n := range nodes {
			ids = append(ids, n.ID)
		}
		sort.Slice(ids, func(i, j int) bool {
			if load[ids[i]] != load[ids[j]] {
				return load[ids[i]] < load[ids[j]]
			}
			return ids[i] < ids[j]
		})
		return ids
	}

	routes := make([]ShardRoute, numShards)
	for shard := 0; shard < numShards; shard++ {
		ids := byLoad()
		r := ShardRoute{Primary: ids[0]}
		load[ids[0]]++
		// 副本：次少负载节点，排除已选节点
		for k := 0; k < numReplicas; k++ {
			ids = byLoad()
			picked := uint64(0)
			for _, id := range ids {
				if id != r.Primary && !contains(r.Replicas, id) {
					picked = id
					break
				}
			}
			if picked == 0 {
				break // 节点不足，副本降级
			}
			r.Replicas = append(r.Replicas, picked)
			load[picked]++
		}
		routes[shard] = r
	}
	return routes, nil
}

func contains(ids []uint64, id uint64) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}
