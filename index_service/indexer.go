package index_service

import "github.com/jmh000527/criker-search/types"

// Indexer Sentinel（分布式grpc的哨兵）和 LocalIndexer（单机索引）都实现了该接口
type Indexer interface {
	AddDoc(doc types.Document) (int, error)
	DeleteDoc(docId string) int
	Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []*types.Document
	Count() int
	Close() error
}
