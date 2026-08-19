// Package scorer 内置打分器插件。
//
//	bm25  BM25 相关度打分（k1=1.2, b=0.75）
package scorer

import (
	"math"

	"github.com/FalconEngine/falcon/plugin"
)

// BM25 参数（Lucene 默认值）
const (
	k1 = 1.2
	b  = 0.75
)

// bm25 打分器：
//
//	IDF = ln(1 + (N - df + 0.5) / (df + 0.5))
//	score = IDF * tf*(k1+1) / (tf + k1*(1-b+b*dl/avgdl))
type bm25 struct{}

func (bm25) Name() string { return "bm25" }

func (bm25) Score(tf, dl int64, df, n int, avgdl float64) float64 {
	idf := math.Log(1 + (float64(n)-float64(df)+0.5)/(float64(df)+0.5))
	ratio := 0.0
	if avgdl > 0 {
		ratio = float64(dl) / avgdl
	}
	k := k1 * (1 - b + b*ratio)
	return idf * float64(tf) * (k1 + 1) / (float64(tf) + k)
}

func init() {
	plugin.RegisterScorer(bm25{})
}
