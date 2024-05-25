package common

import (
	"context"
	"criker-search/demo"
	indexservice "criker-search/index_service/interface"
)

type VideoSearchContext struct {
	Ctx     context.Context       // 上下文参数
	Indexer indexservice.IIndexer // 索引。可能是本地的Indexer，也可能是分布式的Sentinel
	Request *demo.SearchRequest   // 搜索请求
	Videos  []*demo.BiliVideo     // 搜索结果
}

type UN string
