package utils

// MapEntry ConcurrentHashMap中的一个<key, val>键值对
type MapEntry struct {
	Key   string // 键
	Value any    // 值
}

// MapIterator 迭代器模式接口，定义了迭代器的基本操作
type MapIterator interface {
	// Next 获取 ConcurrentHashMap 中的下一个 <key, val> 键值对
	Next() *MapEntry
}

// ConcurrentHashMapIterator 实现了 MapIterator 接口，提供了对 ConcurrentHashMap 的迭代功能
type ConcurrentHashMapIterator struct {
	cm       *ConcurrentHashMap // 目标 ConcurrentHashMap
	keys     [][]string         // 固定所有 keys 为有序状态
	rowIndex int                // 当前行索引
	colIndex int                // 当前列索引
}

// Next 获取 ConcurrentHashMap 中的下一个 <key, val> 键值对
//
// 返回值:
//   - *MapEntry: 包含当前键值对的 MapEntry 对象。如果迭代器已经遍历完所有键值对，则返回 nil。
func (iter *ConcurrentHashMapIterator) Next() *MapEntry {
	// 行号越界，Next不存在，直接返回
	if iter.rowIndex >= len(iter.keys) {
		return nil
	}
	// 取得当前行存储的 keys
	row := iter.keys[iter.rowIndex]
	// 当前行为空
	if len(row) == 0 {
		// 进入下一行
		iter.rowIndex++
		// 进入递归，因为下一行可能依然为空
		return iter.Next()
	}
	// 根据列号取得一个 key
	key := row[iter.colIndex]
	// 获取该 key 对应的 value
	val, _ := iter.cm.Get(key)
	// 下标指向下一个 key
	if iter.colIndex >= len(row)-1 {
		iter.rowIndex++
		iter.colIndex = 0
	} else {
		iter.colIndex++
	}
	return &MapEntry{
		Key:   key,
		Value: val,
	}
}
