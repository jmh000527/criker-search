package filter

import (
	"criker-search/demo"
	"criker-search/demo/video_search/common"
)

// ViewFilter 按照播放量进行过滤
type ViewFilter struct{}

func (ViewFilter) Apply(ctx *common.VideoSearchContext) {
	request := ctx.Request
	if request == nil {
		return
	}
	if request.ViewFrom > 0 && request.ViewTo > 0 && request.ViewFrom >= request.ViewTo {
		return
	}
	videos := make([]*demo.BiliVideo, 0, len(ctx.Videos))
	for _, video := range ctx.Videos {
		if video.View < int32(request.ViewFrom) {
			continue
		}
		if int32(request.ViewTo) > 0 && video.View > int32(request.ViewTo) {
			continue
		}
		videos = append(videos, video)
	}
	ctx.Videos = videos
}
