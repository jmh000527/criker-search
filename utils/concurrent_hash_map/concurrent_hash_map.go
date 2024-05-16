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

// NewConcurrentHashMap cap预估ConcurrentHashMap中总共容纳多少元素，seg表示ConcurrentHashMap内部划分成几个小map
func NewConcurrentHashMap(seg, cap int) *ConcurrentHashMap {
	mps := make([]map[string]any, seg)
	locks := make([]sync.RWMutex, seg)
	for i := 0; i < seg; i++ {
		mps[i] = make(map[string]any, cap/seg)
	}
	return &ConcurrentHashMap{
		mps:   mps,
		seg:   seg,
		locks: locks,
		seed:  0,
	}
}

// Set 写入<key, value>
func (m *ConcurrentHashMap) Set(key string, value any) {
	index := m.getSegIndex(key)
	m.locks[index].Lock()
	defer m.locks[index].Unlock()
	m.mps[index][key] = value
}

// Get 根据key获取value
func (m *ConcurrentHashMap) Get(key string) (any, bool) {
	index := m.getSegIndex(key)
	m.locks[index].RLock()
	defer m.locks[index].RUnlock()
	value, ok := m.mps[index][key]
	return value, ok
}

// CreateIterator 迭代器初始化
func (m *ConcurrentHashMap) CreateIterator() *ConcurrentHashMapIterator {
	keys := make([][]string, 0, len(m.mps))
	for _, mp := range m.mps {
		// 取得mp下所有的key
		row := maps.Keys(mp)
		keys = append(keys, row)
	}
	return &ConcurrentHashMapIterator{
		cm:       m,
		keys:     keys,
		rowIndex: 0,
		colIndex: 0,
	}
}

// 给出key应该存入哪个小map
func (m *ConcurrentHashMap) getSegIndex(key string) int {
	hash := int(farmhash.Hash32WithSeed([]byte(key), m.seed))
	return hash % m.seg
}
