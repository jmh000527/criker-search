package video_search

import (
	"github.com/jmh000527/criker-search/demo/video_search/filter"
	"github.com/jmh000527/criker-search/demo/video_search/recaller"
)

// UpVideoSearcher 是 up 主视频搜索器，继承自 VideoSearcher。
type UpVideoSearcher struct {
	VideoSearcher
}

// NewUpVideoSearcher 创建一个新的 up 主视频搜索器。
func NewUpVideoSearcher() *UpVideoSearcher {
	searcher := &UpVideoSearcher{}
	searcher.WithRecallers(&recaller.KeywordAuthorRecaller{})
	searcher.WithFilters(&filter.ViewFilter{})
	return searcher
}
