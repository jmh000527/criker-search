package filter

import "criker-search/demo/video_search/common"

// Filter 定义了视频搜索结果过滤器的接口。
type Filter interface {
	Apply(*common.VideoSearchContext)
}
