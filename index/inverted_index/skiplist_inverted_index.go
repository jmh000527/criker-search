package inverted_index

import (
	"github.com/huandu/skiplist"
	"github.com/jmh000527/criker-search/types"
	"github.com/jmh000527/criker-search/utils/concurrent_hash_map"
	farmhash "github.com/leemcloughlin/gofarmhash"
	"runtime"
	"sync"
)

// SkipListInvertedIndexer 表示一个使用跳表作为值的倒排索引。
// 整体上，它是一个 map，其值是一个跳表（SkipList）。
type SkipListInvertedIndexer struct {
	table *utils.ConcurrentHashMap // 使用分段锁保护的并发安全 map，用于存储倒排索引的数据
	locks []sync.RWMutex           // 针对相同的 key 进行竞争的锁，以确保在修改倒排索引时的并发安全
}

// SkipListValue 跳表的key是Document IntId，跳表的value是SkipListValue类型
type SkipListValue struct {
	Id          string // 业务侧的ID
	BitsFeature uint64 // 文件属性位图
}

// NewSkipListInvertedIndexer 创建并返回一个新的 SkipListInvertedIndexer 实例。
// 该实例用于管理一个倒排索引，使用跳表作为索引值，并支持并发操作。
//
// 参数:
//   - docNumEstimate: 预估的文档数量，用于初始化并发哈希表的容量。
//
// 返回值:
//   - *SkipListInvertedIndexer: 一个新的 SkipListInvertedIndexer 实例。
func NewSkipListInvertedIndexer(docNumEstimate int) *SkipListInvertedIndexer {
	indexer := &SkipListInvertedIndexer{
		// 创建一个分段锁保护的并发安全 map，用于存储倒排索引的数据。
		table: utils.NewConcurrentHashMap(runtime.NumCPU(), docNumEstimate),
		// 创建一个大小为 1000 的 RWMutex 数组，用于锁定倒排索引中的不同 key，以确保并发安全。
		locks: make([]sync.RWMutex, 1000),
	}
	return indexer
}

// Add 将一个 Document 添加到倒排索引中。
//
// 参数:
//   - doc: 需要添加的文档，类型为 types.Document。
func (indexer *SkipListInvertedIndexer) Add(doc types.Document) {
	for _, keyword := range doc.Keywords {
		// 获取倒排索引的 key，通常是关键词的字符串表示
		key := keyword.ToString()
		// 获取与 key 关联的锁，用于确保并发操作的安全性
		lock := indexer.getLock(key)
		// 创建跳表中的值，包括文档的 ID 和位特征
		skipListValue := SkipListValue{
			Id:          doc.Id,
			BitsFeature: doc.BitsFeature,
		}

		lock.Lock()
		if value, exists := indexer.table.Get(key); exists {
			// 如果倒排索引的 key 已存在，从表中获取对应的跳表，并将新文档的 ID 和位特征添加到跳表中
			list := value.(*skiplist.SkipList)
			list.Set(doc.IntId, skipListValue)
		} else {
			// 如果倒排索引的 key 不存在，创建一个新的跳表，并将文档添加到该跳表中
			list := skiplist.New(skiplist.Uint64)
			list.Set(doc.IntId, skipListValue)
			// 将新的跳表存入倒排索引表中，并发安全
			indexer.table.Set(key, list)
		}
		lock.Unlock()
	}
}

// Delete 从倒排索引中删除与给定关键词和文档 ID 关联的文档。
//
// 参数:
//   - keyword: 要删除的文档的关键词，类型为 *types.Keyword。
//   - IntId: 要删除的文档的唯一标识符，类型为 uint64。
func (indexer *SkipListInvertedIndexer) Delete(keyword *types.Keyword, IntId uint64) {
	// 将关键词转换为字符串，作为倒排索引的 key。
	key := keyword.ToString()
	// 获取与 key 关联的锁，以确保并发修改的安全。
	lock := indexer.getLock(key)
	lock.Lock()
	defer lock.Unlock()
	// 如果倒排索引中存在该 key，获取对应的跳表并从中删除文档。
	if value, exists := indexer.table.Get(key); exists {
		list := value.(*skiplist.SkipList)
		list.Remove(IntId)
	}
}

// Search 执行搜索查询并返回业务侧文档ID列表。
// 该方法调用内部的 search 方法，获取匹配的文档 ID 和其 SkipListValue。
// 然后将匹配的文档 ID 转换为业务侧 ID 并返回。
//
// 参数:
//   - query: 查询条件，类型为 *types.TermQuery。
//   - onFlag: 需要匹配的特征位标志，类型为 uint64。
//   - offFlag: 需要排除的特征位标志，类型为 uint64。
//   - orFlags: 需要匹配的多个或标志，类型为 []uint64。
//
// 返回值:
//   - []string: 符合查询条件的业务侧文档ID列表。如果没有匹配的文档，则返回 nil。
func (indexer *SkipListInvertedIndexer) Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []string {
	// 执行搜索并获取匹配的 SkipList
	result := indexer.search(query, onFlag, offFlag, orFlags)
	if result == nil {
		return nil
	}

	// 创建一个切片，用于存储业务侧文档ID
	arr := make([]string, 0, result.Len())

	// 获取跳表的第一个节点
	node := result.Front()
	// 遍历匹配的结果，将文档ID添加到切片中
	for node != nil {
		skipListValue := node.Value.(SkipListValue)
		arr = append(arr, skipListValue.Id)
		node = node.Next()
	}

	// 返回业务侧文档ID列表
	return arr
}

// FilterByBits 根据 bits 特征进行过滤。
// 该方法检查传入的 bits 是否符合指定的过滤条件。
// - `onFlag`：所有 bits 必须完全匹配 `onFlag`。
// - `offFlag`：所有 bits 必须完全不匹配 `offFlag`。
// - `orFlags`：bits 必须匹配 `orFlags` 列表中的所有标志中的至少一个。
//
// 参数:
//   - bits: 需要进行过滤的特征位，类型为 uint64。
//   - onFlag: 所有 bits 必须匹配的标志，类型为 uint64。
//   - offFlag: 所有 bits 必须不匹配的标志，类型为 uint64。
//   - orFlags: bits 必须匹配的多个标志中的任意一个，类型为 []uint64。
//
// 返回值:
//   - bool: 如果 bits 满足所有过滤条件，返回 true；否则返回 false。
func (indexer *SkipListInvertedIndexer) FilterByBits(bits, onFlag, offFlag uint64, orFlags []uint64) bool {
	// 检查 bits 是否包含 onFlag 中所有的位。
	if bits&onFlag != onFlag {
		return false
	}
	// 检查 bits 是否包含 offFlag 中的任何位。
	if bits&offFlag != uint64(0) {
		return false
	}
	// 检查 bits 是否匹配或标志列表中的所有标志中的至少一个。
	for _, orFlag := range orFlags {
		// 只要有一个 orFlag 的位在 bits 中存在，就符合条件。
		if orFlag > 0 && bits&orFlag <= 0 {
			return false
		}
	}
	return true
}

// search 执行 TermQuery 查询并返回匹配的跳表结果。
// 该方法根据查询条件 q、特征位标志 onFlag、offFlag 以及或标志 orFlags 从倒排索引中查找符合条件的文档 ID。
// 返回的跳表包含所有匹配的文档 ID 和其对应的 SkipListValue。
//
// 参数:
//   - q: 查询条件，类型为 *types.TermQuery。
//   - onFlag: 需要匹配的特征位标志，类型为 uint64。
//   - offFlag: 需要排除的特征位标志，类型为 uint64。
//   - orFlags: 需要匹配的多个或标志，类型为 []uint64。
//
// 返回值:
//   - *skiplist.SkipList: 匹配的文档 ID 和其对应的 SkipListValue。
func (indexer *SkipListInvertedIndexer) search(q *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) *skiplist.SkipList {
	// 处理叶子节点情况，即直接根据关键词查找。
	if q.Keyword != nil {
		// 获取关键词对应的跳表。
		keyword := q.Keyword.ToString()
		// 如果关键词存在，获取对应的跳表。
		if value, exists := indexer.table.Get(keyword); exists {
			list := value.(*skiplist.SkipList)
			result := skiplist.New(skiplist.Uint64) // 存储查询结果的跳表

			// 获取跳表的第一个节点
			node := list.Front()
			// 遍历跳表，查找符合条件的文档
			for node != nil {
				intId := node.Key().(uint64)
				skipListValue := node.Value.(SkipListValue)
				flag := skipListValue.BitsFeature
				// 根据特征位标志过滤结果
				if intId > 0 && indexer.FilterByBits(flag, onFlag, offFlag, orFlags) {
					result.Set(intId, skipListValue)
				}
				node = node.Next()
			}

			return result
		}
	} else if len(q.Must) > 0 {
		// 处理 Must 查询条件，将所有 Must 查询的结果进行交集运算
		results := make([]*skiplist.SkipList, 0, len(q.Must))
		for _, query := range q.Must {
			// 递归执行 Must 查询
			results = append(results, indexer.search(query, onFlag, offFlag, orFlags))
		}
		// 计算 Must 查询结果的交集
		return IntersectionOfSkipLists(results...)
	} else if len(q.Should) > 0 {
		// 处理 Should 查询条件，将所有 Should 查询的结果进行并集运算
		results := make([]*skiplist.SkipList, 0, len(q.Should))
		for _, query := range q.Should {
			// 递归执行 Should 查询
			results = append(results, indexer.search(query, onFlag, offFlag, orFlags))
		}
		// 计算 Should 查询结果的并集
		return IntersectionOfSkipLists(results...)
	}
	// 如果查询条件为空，返回 nil
	return nil
}

// getLock 获取与给定 key 关联的读写锁。
// 使用哈希值来确定锁的索引，以确保相同的 key 总是使用相同的锁。
// 这样可以在并发修改时确保对相同 key 的操作是线程安全的。
//
// 参数:
//   - key: 需要获取锁的键，类型为字符串。
//
// 返回值:
//   - *sync.RWMutex: 与给定 key 关联的读写锁。
func (indexer *SkipListInvertedIndexer) getLock(key string) *sync.RWMutex {
	// 使用 farmhash 哈希函数计算 key 的哈希值，并将其转换为整数。
	n := int(farmhash.Hash32WithSeed([]byte(key), 0))
	// 根据哈希值确定锁的索引，并返回对应的锁。
	return &indexer.locks[n%len(indexer.locks)]
}
