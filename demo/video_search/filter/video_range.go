package filter

import (
	"criker-search/demo"
	"criker-search/demo/video_search/common"
)

// ViewFilter 按照播放量进行过滤。
type ViewFilter struct{}

// Apply 应用播放量过滤器到视频搜索上下文。
func (ViewFilter) Apply(ctx *common.VideoSearchContext) {
	// 获取搜索请求
	request := ctx.Request
	if request == nil {
		return
	}
	// 创建一个新的视频切片，用于存储过滤后的视频
	videos := make([]*demo.BiliVideo, 0, len(ctx.Videos))
	// 如果播放量范围不合法，则不进行过滤
	if request.ViewFrom > 0 && request.ViewTo > 0 && request.ViewFrom >= request.ViewTo {
		// 返回空切片
		ctx.Videos = videos
	}
	// 遍历搜索结果中的每个视频
	for _, video := range ctx.Videos {
		// 如果视频的播放量小于搜索请求中指定的最小播放量，则跳过该视频
		if video.View < int32(request.ViewFrom) {
			continue
		}
		// 如果搜索请求中指定了最大播放量，并且视频的播放量超过了最大播放量，则跳过该视频
		if int32(request.ViewTo) > 0 && video.View > int32(request.ViewTo) {
			continue
		}
		// 否则将视频加入到过滤后的视频列表中
		videos = append(videos, video)
	}
	// 更新视频搜索上下文中的视频列表为过滤后的视频列表
	ctx.Videos = videos
}
