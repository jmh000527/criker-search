package video_search

import (
	"github.com/jmh000527/criker-search/demo"
	"github.com/jmh000527/criker-search/demo/video_search/common"
	"github.com/jmh000527/criker-search/demo/video_search/filter"
	"github.com/jmh000527/criker-search/demo/video_search/recaller"
	"github.com/jmh000527/criker-search/utils"
	"golang.org/x/exp/maps"
	"reflect"
	"sync"
	"time"
)

// VideoSearcher 是视频搜索器的模板方法，负责组织召回器和过滤器。
type VideoSearcher struct {
	Recallers []recaller.Recaller // 视频搜索结果召回器列表
	Filters   []filter.Filter     // 视频搜索结果过滤器列表
}

// WithRecallers 向视频搜索器添加一个或多个视频搜索结果召回器。
func (vs *VideoSearcher) WithRecallers(recallers ...recaller.Recaller) {
	vs.Recallers = append(vs.Recallers, recallers...)
}

// WithFilters 向视频搜索器添加一个或多个视频搜索结果过滤器。
func (vs *VideoSearcher) WithFilters(filters ...filter.Filter) {
	vs.Filters = append(vs.Filters, filters...)
}

// Recall 执行视频搜索结果召回，调用各个召回器的 Recall 方法，并将结果合并到搜索上下文中。
func (vs *VideoSearcher) Recall(searchContext *common.VideoSearchContext) {
	// 如果没有召回器，则直接返回
	if len(vs.Recallers) == 0 {
		return
	}
	// 用于收集召回的视频结果
	collection := make(chan *demo.BiliVideo, 1000)
	// 用于等待所有召回器完成
	wg := sync.WaitGroup{}
	wg.Add(len(vs.Recallers))
	// 并发执行每个召回器的召回任务
	for _, r := range vs.Recallers {
		go func(recaller recaller.Recaller) {
			defer wg.Done()
			// 获取召回器的名称
			rule := reflect.TypeOf(recaller).Elem().Name()
			// 调用召回器的 Recall 方法，获取召回结果
			result := recaller.Recall(searchContext)
			utils.Log.Printf("召回 %d 个文档，使用规则 %s", len(result), rule)
			// 将召回的视频结果发送到通道中
			for _, video := range result {
				collection <- video
			}
		}(r)
	}

	signalChan := make(chan struct{})
	// 用于合并多路召回的视频结果
	videoMap := make(map[string]*demo.BiliVideo, 1000)
	// 启动一个 goroutine 用于收集召回结果，并将结果合并到搜索上下文中
	go func() {
		for {
			video, ok := <-collection
			if !ok {
				break
			}
			videoMap[video.Id] = video
		}
		// 发送信号通知收集任务完成
		signalChan <- struct{}{}
	}()
	// 等待所有召回任务完成
	wg.Wait()
	// 关闭结果通道
	close(collection)
	// 等待结果收集任务完成
	<-signalChan
	// 将结果 map 中的值转换为切片，更新搜索上下文中的视频列表
	searchContext.Videos = maps.Values(videoMap)
}

// Filter 执行视频搜索结果过滤，调用各个过滤器的 Apply 方法，过滤搜索上下文中的视频。
//
// 参数:
//   - searchContext: 包含搜索上下文信息的 VideoSearchContext 对象。该对象包含了召回的文档以及用于过滤的相关信息。
//
// 返回值:
//   - 无。此方法会直接修改传入的 searchContext 对象，过滤掉不符合条件的视频。
func (vs *VideoSearcher) Filter(searchContext *common.VideoSearchContext) {
	for _, f := range vs.Filters {
		f.Apply(searchContext)
	}
}

// Search 执行视频搜索，包含召回和过滤两个步骤。
//
// 参数:
//   - searchContext: 包含搜索上下文信息的VideoSearchContext对象。
//
// 返回值:
//   - []*demo.BiliVideo: 经过召回和过滤后的BiliVideo对象切片。
func (vs *VideoSearcher) Search(searchContext *common.VideoSearchContext) []*demo.BiliVideo {
	t1 := time.Now()
	// 执行召回操作
	vs.Recall(searchContext)
	t2 := time.Now()
	utils.Log.Printf("召回 %d 个文档，用时 %d 毫秒", len(searchContext.Videos), t2.Sub(t1).Milliseconds())

	// 执行过滤操作
	vs.Filter(searchContext)
	t3 := time.Now()
	utils.Log.Printf("过滤后剩余 %d 个文档，用时 %d 毫秒", len(searchContext.Videos), t3.Sub(t2).Milliseconds())

	return searchContext.Videos
}
