package recaller

import (
	"criker-search/demo"
	"criker-search/demo/video_search/common"
	"criker-search/types"
	"github.com/gogo/protobuf/proto"
	"strings"
)

// KeywordRecaller 根据关键词进行回调，用于全站搜索。
type KeywordRecaller struct{}

// Recall 根据关键词进行视频检索，并返回符合条件的视频列表。
func (KeywordRecaller) Recall(ctx *common.VideoSearchContext) []*demo.BiliVideo {
	// 获取搜索请求
	request := ctx.Request
	if request == nil {
		return nil
	}
	// 获取索引服务
	indexer := ctx.Indexer
	if indexer == nil {
		return nil
	}
	// 获取搜索关键词
	keywords := request.Keywords
	// 创建查询对象
	query := new(types.TermQuery)
	// 如果有关键词，则构建关键词查询条件
	if len(keywords) > 0 {
		for _, word := range keywords {
			query = query.And(types.NewTermQuery("content", word)) // 满足关键词
		}
	}
	// 如果指定了作者，则添加作者查询条件
	if len(request.Author) > 0 {
		query = query.And(types.NewTermQuery("author", strings.ToLower(request.Author))) // 满足作者
	}
	// 构建或逻辑查询条件，满足指定类别
	orFlags := []uint64{demo.GetClassBits(request.Classes)}
	// 执行查询，获取匹配的文档
	docs := indexer.Search(query, 0, 0, orFlags)
	// 创建一个用于存储匹配视频的切片
	videos := make([]*demo.BiliVideo, 0, len(docs))
	// 遍历匹配的文档，反序列化为视频对象，加入到视频列表中
	for _, doc := range docs {
		var video demo.BiliVideo
		if err := proto.Unmarshal(doc.Bytes, &video); err == nil {
			videos = append(videos, &video)
		}
	}
	return videos
}
