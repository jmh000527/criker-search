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

type Recaller interface {
	Recall(*common.VideoSearchContext) []*demo.BiliVideo
}

type Filter interface {
	Apply(*common.VideoSearchContext)
}

// VideoSearcher 模板方法Template Method模式。超类
type VideoSearcher struct {
	Recallers []Recaller // 实际中，除了正常的关键词召回外，可能还要召回广告
	Filters   []Filter
}

func (vs *VideoSearcher) WithRecallers(recallers ...Recaller) {
	vs.Recallers = append(vs.Recallers, recallers...)
}

func (vs *VideoSearcher) WithFilters(filters ...Filter) {
	vs.Filters = append(vs.Filters, filters...)
}

func (vs *VideoSearcher) Recall(searchContext *common.VideoSearchContext) {
	if len(vs.Recallers) == 0 {
		return
	}
	// 并发执行多路召回
	collection := make(chan *demo.BiliVideo, 1000)
	wg := sync.WaitGroup{}
	wg.Add(len(vs.Recallers))
	for _, r := range vs.Recallers {
		go func(recaller Recaller) {
			defer wg.Done()
			rule := reflect.TypeOf(recaller).Name()
			result := recaller.Recall(searchContext)
			utils.Log.Printf("recall %d talents by %s", len(result), rule)
			for _, video := range result {
				collection <- video
			}

		}(r)
	}
	// 通过map合并多路召回的结果
	videoMap := make(map[string]*demo.BiliVideo, 1000)
	signalChan := make(chan struct{})
	go func() {
		for {
			video, ok := <-collection
			if !ok {
				break
			}
			videoMap[video.Id] = video
		}
		signalChan <- struct{}{}
	}()
	wg.Wait()
	close(collection)
	<-signalChan
	searchContext.Videos = maps.Values(videoMap)
}

func (vs *VideoSearcher) Filter(searchContext *common.VideoSearchContext) {
	// 顺序执行各个过滤规则
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

// AllVideoSearcher 子类
type AllVideoSearcher struct {
	VideoSearcher
}

func NewAllVideoSearcher() *AllVideoSearcher {
	searcher := new(AllVideoSearcher)
	searcher.WithRecallers(recaller.KeywordRecaller{})
	searcher.WithFilters(filter.ViewFilter{})
	return searcher
}

// UpVideoSearcher 子类
type UpVideoSearcher struct {
	VideoSearcher
}

func NewUpVideoSearcher() *UpVideoSearcher {
	searcher := new(UpVideoSearcher)
	searcher.WithRecallers(recaller.KeywordAuthorRecaller{})
	searcher.WithFilters(filter.ViewFilter{})
	return searcher
}
