// Package plugins 汇总全部内置插件，import 本包即完成注册。
// 外部插件由使用方自行 import（进程内注册模式，同 database/sql 驱动）。
package plugins

import (
	_ "github.com/FalconEngine/falcon/plugins/agg"       // 聚合：terms/min/max/avg/sum/cardinality
	_ "github.com/FalconEngine/falcon/plugins/analyzer"  // 分析链组件与分词器：standard/keyword/whitespace/stop 等
	_ "github.com/FalconEngine/falcon/plugins/fieldtype" // 字段类型：text/keyword/number/date/bool/stored
	_ "github.com/FalconEngine/falcon/plugins/query"     // 查询子句：match/term/terms/range/bool/match_all/ids
	_ "github.com/FalconEngine/falcon/plugins/scorer"    // 打分器：bm25
)
