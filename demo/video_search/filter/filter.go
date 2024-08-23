package filter

import "github.com/jmh000527/criker-search/demo/video_search/common"

// Filter 定义了视频搜索结果过滤器的接口。
type Filter interface {
	Apply(*common.VideoSearchContext)
}
