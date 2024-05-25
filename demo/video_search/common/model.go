package common

import (
	"context"
	"criker-search/demo"
	indexservice "criker-search/index_service/interface"
)

// VideoSearchContext 视频搜索的上下文。
type VideoSearchContext struct {
	Ctx     context.Context       // 上下文参数，用于传递上下文信息，如超时、取消信号等
	Indexer indexservice.IIndexer // 索引服务，可能是本地的 Indexer，也可能是分布式的 Sentinel，提供索引操作的方法
	Request *demo.SearchRequest   // 搜索请求，包含了搜索的具体参数，如关键词、作者等
	Videos  []*demo.BiliVideo     // 搜索结果，存储搜索得到的视频信息
}

// UN 表示用户名。
type UN string
