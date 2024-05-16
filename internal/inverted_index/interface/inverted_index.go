package inverted_index

import (
	"criker-search/types"
)

type InvertedIndexer interface {
	Add(doc types.Document)                                                              //添加一个doc
	Delete(keyword *types.Keyword, IntId uint64)                                         //从key上删除对应的doc
	Search(q *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []string //查找，返回业务侧文档ID
}
