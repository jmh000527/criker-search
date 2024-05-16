package utils

// MapEntry ConcurrentHashMap中的一个<key, val>键值对
type MapEntry struct {
	Key   string
	Value any
}

// MapIterator 迭代器模式
type MapIterator interface {
	Next() *MapEntry
}

type ConcurrentHashMapIterator struct {
	cm       *ConcurrentHashMap
	keys     [][]string // 固定所有keys为有序状态
	rowIndex int
	colIndex int
}

// Next 获取ConcurrentHashMap中的下一个<key, val>键值对
func (iter *ConcurrentHashMapIterator) Next() *MapEntry {
	// 行号越界，Next不存在，直接返回
	if iter.rowIndex >= len(iter.keys) {
		return nil
	}
	// 取得当前行存储的keys
	row := iter.keys[iter.rowIndex]
	// 当前行为空
	if len(row) == 0 {
		// 进入下一行
		iter.rowIndex++
		// 进入递归，因为下一行可能依然为空
		return iter.Next()
	}
	// 根据列号取得一个key
	key := row[iter.colIndex] // 即使下标为零，当切片为空时依然会出现下标越界
	// 获取该key对应的value
	val, _ := iter.cm.Get(key)
	// 下标指向下一个key
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
