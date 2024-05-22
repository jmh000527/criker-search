package inverted_index

import (
	"criker-search/types"
	"criker-search/utils/concurrent_hash_map"
	"github.com/huandu/skiplist"
	farmhash "github.com/leemcloughlin/gofarmhash"
	"runtime"
	"sync"
)

// SkipListInvertedIndexer 倒排索引整体上是个map，map的value是一个SkipList
type SkipListInvertedIndexer struct {
	table *utils.ConcurrentHashMap // 分段map，并发安全
	locks []sync.RWMutex           // 修改倒排索引时，相同的key需要去竞争同一把锁
}

// SkipListValue 跳表的key是Document IntId，跳表的value是SkipListValue类型
type SkipListValue struct {
	Id          string // 业务侧的ID
	BitsFeature uint64 // 文件属性位图
}

func NewSkipListInvertedIndexer(docNumEstimate int) *SkipListInvertedIndexer {
	indexer := &SkipListInvertedIndexer{
		table: utils.NewConcurrentHashMap(runtime.NumCPU(), docNumEstimate),
		locks: make([]sync.RWMutex, 1000),
	}
	return indexer
}

// Add 添加一个Document到倒排索引中去
func (indexer *SkipListInvertedIndexer) Add(doc types.Document) {
	for _, keyword := range doc.Keywords {
		// 倒排索引的key
		key := keyword.ToString()
		lock := indexer.getLock(key)
		skipListValue := SkipListValue{
			Id:          doc.Id,
			BitsFeature: doc.BitsFeature,
		}

		lock.Lock()
		if value, exists := indexer.table.Get(key); exists {
			// 倒排索引的key存在，获取对应value（跳表），并存入跳表
			list := value.(*skiplist.SkipList)
			list.Set(doc.IntId, skipListValue)
		} else {
			// 倒排索引的key不存在，创建一个跳表作为value
			list := skiplist.New(skiplist.Uint64)
			list.Set(doc.IntId, skipListValue)
			// 并发安全
			indexer.table.Set(key, list)
		}
		lock.Unlock()
	}
}

// Delete 从倒排索引中删除一个Document
func (indexer *SkipListInvertedIndexer) Delete(keyword *types.Keyword, IntId uint64) {
	key := keyword.ToString()
	lock := indexer.getLock(key)
	lock.Lock()
	defer lock.Unlock()
	if value, exists := indexer.table.Get(key); exists {
		list := value.(*skiplist.SkipList)
		list.Remove(IntId)
	}
}

// Search 搜索，返回docId（业务侧ID）
func (indexer *SkipListInvertedIndexer) Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []string {
	result := indexer.search(query, onFlag, offFlag, orFlags)
	if result == nil {
		return nil
	}
	arr := make([]string, 0, result.Len()) // 存储业务侧ID
	node := result.Front()
	for node != nil {
		skipListValue := node.Value.(SkipListValue)
		arr = append(arr, skipListValue.Id)
		node = node.Next()
	}
	return arr
}

// FilterByBits 按照bits特征进行过滤
func (indexer *SkipListInvertedIndexer) FilterByBits(bits, onFlag, offFlag uint64, orFlags []uint64) bool {
	// onFlag所有bits必须全部命中
	if bits&onFlag != onFlag {
		return false
	}
	// offFlag所有bits必须全部不命中
	if bits&offFlag != uint64(0) {
		return false
	}
	// 多个orFlag必须全部命中
	for _, orFlag := range orFlags {
		// 单个orFlag只要有一个bit命中即可
		if orFlag > 0 && bits&orFlag <= 0 {
			return false
		}
	}
	return true
}

func (indexer *SkipListInvertedIndexer) search(q *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) *skiplist.SkipList {
	// 叶子节点
	if q.Keyword != nil {
		keyword := q.Keyword.ToString()
		if value, exists := indexer.table.Get(keyword); exists {
			list := value.(*skiplist.SkipList)
			result := skiplist.New(skiplist.Uint64) // 存储返回结果
			node := list.Front()
			for node != nil {
				intId := node.Key().(uint64)
				skipListValue := node.Value.(SkipListValue)
				flag := skipListValue.BitsFeature
				if intId > 0 && indexer.FilterByBits(flag, onFlag, offFlag, orFlags) {
					result.Set(intId, skipListValue)
				}
				node = node.Next()
			}
			return result
		}
	} else if len(q.Must) > 0 {
		results := make([]*skiplist.SkipList, 0, len(q.Must))
		for _, query := range q.Must {
			results = append(results, indexer.search(query, onFlag, offFlag, orFlags))
		}
		return IntersectionOfSkipLists(results...)
	} else if len(q.Should) > 0 {
		results := make([]*skiplist.SkipList, 0, len(q.Should))
		for _, query := range q.Should {
			results = append(results, indexer.search(query, onFlag, offFlag, orFlags))
		}
		return IntersectionOfSkipLists(results...)
	}
	return nil
}

// 获取某个key对应的读写锁
func (indexer *SkipListInvertedIndexer) getLock(key string) *sync.RWMutex {
	n := int(farmhash.Hash32WithSeed([]byte(key), 0))
	return &indexer.locks[n%len(indexer.locks)]
}
