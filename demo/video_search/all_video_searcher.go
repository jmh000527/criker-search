package video_search

import (
	"criker-search/demo/video_search/filter"
	"criker-search/demo/video_search/recaller"
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
