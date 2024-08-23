package inverted_index

import (
	"github.com/jmh000527/criker-search/types"
)

// InvertedIndexer 定义了倒排索引器的接口，提供添加文档、删除文档以及根据查询条件搜索文档的功能。
type InvertedIndexer interface {
	// Add 添加一个文档到倒排索引中。
	Add(doc types.Document)

	// Delete 从倒排索引中删除与指定关键词和文档 ID 关联的文档。
	Delete(keyword *types.Keyword, IntId uint64)

	// Search 根据给定的查询条件在倒排索引中查找匹配的文档，并返回业务侧的文档 ID 列表。
	Search(q *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []string
}
