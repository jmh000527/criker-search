package recaller

import (
	"github.com/jmh000527/criker-search/demo"
	"github.com/jmh000527/criker-search/demo/video_search/common"
)

// Recaller 定义了视频搜索结果召回器的接口。
type Recaller interface {
	Recall(*common.VideoSearchContext) []*demo.BiliVideo
}
