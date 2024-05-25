package video_search

import (
	"criker-search/demo"
	"criker-search/demo/video_search/common"
	"criker-search/demo/video_search/filter"
	"criker-search/demo/video_search/recaller"
	"criker-search/utils"
	"golang.org/x/exp/maps"
	"reflect"
	"sync"
	"time"
)

// Recaller 定义了视频搜索结果召回器的接口。
type Recaller interface {
	Recall(*common.VideoSearchContext) []*demo.BiliVideo
}

// Filter 定义了视频搜索结果过滤器的接口。
type Filter interface {
	Apply(*common.VideoSearchContext)
}

// VideoSearcher 是视频搜索器的模板方法，负责组织召回器和过滤器。
type VideoSearcher struct {
	Recallers []Recaller // 视频搜索结果召回器列表
	Filters   []Filter   // 视频搜索结果过滤器列表
}

// WithRecallers 向视频搜索器添加一个或多个视频搜索结果召回器。
func (vs *VideoSearcher) WithRecallers(recallers ...Recaller) {
	vs.Recallers = append(vs.Recallers, recallers...)
}

// WithFilters 向视频搜索器添加一个或多个视频搜索结果过滤器。
func (vs *VideoSearcher) WithFilters(filters ...Filter) {
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
		go func(recaller Recaller) {
			defer wg.Done()
			// 获取召回器的名称
			rule := reflect.TypeOf(recaller).Name()
			// 调用召回器的 Recall 方法，获取召回结果
			result := recaller.Recall(searchContext)
			utils.Log.Printf("recall %d talents by %s", len(result), rule)
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
func (vs *VideoSearcher) Filter(searchContext *common.VideoSearchContext) {
	for _, f := range vs.Filters {
		f.Apply(searchContext)
	}
}

// Search 超类定义了一个算法的框架，在子类中重写特定的算法步骤（即recall和filter这2步）
func (vs *VideoSearcher) Search(searchContext *common.VideoSearchContext) []*demo.BiliVideo {
	t1 := time.Now()
	// 召回
	vs.Recall(searchContext)
	t2 := time.Now()
	utils.Log.Printf("recall %d docs in %d ms", len(searchContext.Videos), t2.Sub(t1).Milliseconds())
	// 过滤
	vs.Filter(searchContext)
	t3 := time.Now()
	utils.Log.Printf("after filter remain %d docs in %d ms", len(searchContext.Videos), t2.Sub(t3).Milliseconds())
	return searchContext.Videos
}

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
