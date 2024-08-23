package utils

import (
	farmhash "github.com/leemcloughlin/gofarmhash"
	"golang.org/x/exp/maps"
	"sync"
)

// ConcurrentHashMap 自行实现并发读写的map。key是string，value是any。
type ConcurrentHashMap struct {
	mps   []map[string]any // 由多个小map组成
	seg   int              // 小map的个数
	locks []sync.RWMutex   // 每个小map分配一把读写锁，避免全局只有一把锁影响性能
	seed  uint32           // 每次执行FarmHash传统一的seed
}

// NewConcurrentHashMap 创建并返回一个新的 ConcurrentHashMap 实例。
// 该实例通过将哈希表划分为多个小的 map 来实现并发安全，适用于高并发环境。
//
// 参数:
//   - seg: 内部划分的小 map 的数量，用于实现并发控制。
//   - cap: 预估的总元素数量，用于计算每个小 map 的初始容量。
//
// 返回值:
//   - *ConcurrentHashMap: 一个新的 ConcurrentHashMap 实例。
func NewConcurrentHashMap(seg, cap int) *ConcurrentHashMap {
	// 创建一个大小为 seg 的 map 数组，每个 map 存储实际的键值对。
	mps := make([]map[string]any, seg)
	// 创建一个大小为 seg 的 RWMutex 数组，用于锁定不同的小 map，实现并发安全。
	locks := make([]sync.RWMutex, seg)
	// 初始化每个小 map 的容量为 cap/seg，分摊总容量。
	for i := 0; i < seg; i++ {
		mps[i] = make(map[string]any, cap/seg)
	}
	// 返回创建的 ConcurrentHashMap 实例。
	return &ConcurrentHashMap{
		mps:   mps,
		seg:   seg,
		locks: locks,
		seed:  0,
	}
}

// Set 写入<key, value>
//
// 参数:
//   - key: 键，表示要存储的key。
//   - value: 值，表示要存储的value。
func (m *ConcurrentHashMap) Set(key string, value any) {
	// 根据key计算出应该存储的分片索引
	index := m.getSegIndex(key)

	// 加锁保护该分片的并发写操作
	m.locks[index].Lock()
	defer m.locks[index].Unlock()

	// 在对应的分片中存储key-value
	m.mps[index][key] = value
}

// Get 根据key获取value
//
// 参数:
//   - key: 键，表示要检索的key。
//
// 返回值:
//   - any: 如果key存在，返回对应的value。
//   - bool: 表示key是否存在。
func (m *ConcurrentHashMap) Get(key string) (any, bool) {
	// 根据key计算出存储的分片索引
	index := m.getSegIndex(key)

	// 加锁保护该分片的并发读操作
	m.locks[index].RLock()
	defer m.locks[index].RUnlock()

	// 从对应的分片中获取value
	value, ok := m.mps[index][key]
	return value, ok
}

// CreateIterator 迭代器初始化
//
// 返回值:
//   - *ConcurrentHashMapIterator: 迭代器对象，用于遍历ConcurrentHashMap中的键值对。
func (m *ConcurrentHashMap) CreateIterator() *ConcurrentHashMapIterator {
	// 创建一个二维字符串切片，用于存储所有分片中的keys
	keys := make([][]string, 0, len(m.mps))
	for _, mp := range m.mps {
		// 获取每个分片中的所有key
		row := maps.Keys(mp)
		keys = append(keys, row)
	}

	// 返回一个新的迭代器，初始化rowIndex和colIndex为0
	return &ConcurrentHashMapIterator{
		cm:       m,
		keys:     keys,
		rowIndex: 0,
		colIndex: 0,
	}
}

// getSegIndex 计算key应该存储在哪个分片中
//
// 参数:
//   - key: 键，表示要计算的key。
//
// 返回值:
//   - int: 计算出的分片索引。
func (m *ConcurrentHashMap) getSegIndex(key string) int {
	// 使用farmhash和seed生成哈希值，然后取模计算分片索引
	hash := int(farmhash.Hash32WithSeed([]byte(key), m.seed))
	return hash % m.seg
}
