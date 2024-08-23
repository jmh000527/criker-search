package video_search

import (
	"github.com/jmh000527/criker-search/demo/video_search/filter"
	"github.com/jmh000527/criker-search/demo/video_search/recaller"
)

// AllVideoSearcher 是全站视频搜索器，继承自 VideoSearcher。
type AllVideoSearcher struct {
	VideoSearcher
}

// NewAllVideoSearcher 创建一个新的全站视频搜索器。
func NewAllVideoSearcher() *AllVideoSearcher {
	searcher := &AllVideoSearcher{}
	searcher.WithRecallers(&recaller.KeywordRecaller{})
	searcher.WithFilters(&filter.ViewFilter{})
	return searcher
}
