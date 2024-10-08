package recaller

import (
	"github.com/gogo/protobuf/proto"
	"github.com/jmh000527/criker-search/demo"
	"github.com/jmh000527/criker-search/demo/video_search/common"
	"github.com/jmh000527/criker-search/types"
	"strings"
)

// KeywordAuthorRecaller 根据关键词和作者进行回调。
type KeywordAuthorRecaller struct{}

// Recall 根据关键词和作者进行视频检索，并返回符合条件的视频列表。
func (KeywordAuthorRecaller) Recall(ctx *common.VideoSearchContext) []*demo.BiliVideo {
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
	// 获取上下文中的用户名
	v := ctx.Ctx.Value(common.UN("user_name"))
	if v != nil {
		if author, ok := v.(string); ok {
			if len(author) > 0 {
				query = query.And(types.NewTermQuery("author", strings.ToLower(author))) // 满足作者
			}
		}
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
