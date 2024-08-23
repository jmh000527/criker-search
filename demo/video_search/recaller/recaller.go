package recaller

import (
	"criker-search/demo"
	"criker-search/demo/video_search/common"
)

// Recaller 定义了视频搜索结果召回器的接口。
type Recaller interface {
	Recall(*common.VideoSearchContext) []*demo.BiliVideo
}
