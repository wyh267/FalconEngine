package scorer

import (
	"testing"

	"github.com/FalconEngine/falcon/plugin"
	"github.com/FalconEngine/falcon/plugin/plugintest"
)

func TestBM25(t *testing.T) {
	s, err := plugin.GetScorer("bm25")
	if err != nil {
		t.Fatal(err)
	}
	plugintest.RunScorerSuite(t, s)
}
