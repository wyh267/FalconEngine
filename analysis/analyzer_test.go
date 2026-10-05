package analysis

import (
	"reflect"
	"testing"
)

func TestTokenize(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"hello world", []string{"hello", "world"}},
		{"Hello, World!", []string{"Hello", "World"}}, // 保留原始大小写（转小写由 token filter 负责）
		{"Go语言1.24发布", []string{"Go", "语", "言", "1", "24", "发", "布"}},
		{"看山东，赞山东", []string{"看", "山", "东", "赞", "山", "东"}},
		{"  a\tb\nc  ", []string{"a", "b", "c"}},
	}
	for _, c := range cases {
		got := Tokenize(c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("Tokenize(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
