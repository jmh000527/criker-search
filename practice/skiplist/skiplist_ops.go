package tests

import "github.com/huandu/skiplist"

// IntersectionOfSkipLists 多个SkipList求交集
func IntersectionOfSkipLists(lists ...*skiplist.SkipList) *skiplist.SkipList {
	if len(lists) == 0 {
		return nil
	}
	if len(lists) == 1 {
		return lists[0]
	}
	result := skiplist.New(skiplist.Uint64)
	curNodes := make([]*skiplist.Element, len(lists))
	// 初始化curNodes
	for i, list := range lists {
		// 只要lists中有一条是空链，则交集为空
		if list == nil || list.Len() == 0 {
			return nil
		}
		curNodes[i] = list.Front()
	}
	for {
		// 此刻，哪个指针对应的值最大（最大者可能存在多个，所以用map）
		maxList := make(map[int]struct{}, len(curNodes))
		var maxVal uint64 = 0
		for i, node := range curNodes {
			if node.Key().(uint64) > maxVal {
				maxVal = node.Key().(uint64)
				// 清空，重新赋值maxList，只存储最大的key
				maxList = map[int]struct{}{i: {}}
			} else if node.Key().(uint64) == maxVal {
				maxList[i] = struct{}{}
			}
		}
		// 所有node的值都一样大，则新诞生一个交集
		if len(maxList) == len(curNodes) {
			// 此时所有curNodes的key相同
			result.Set(curNodes[0].Key(), curNodes[0].Value)
			// 所有node均需往后移
			for i, node := range curNodes {
				curNodes[i] = node.Next()
				// 有指针遍历完，不可能再有新交集
				if curNodes[i] == nil {
					return result
				}
			}
		} else {
			for i, node := range curNodes {
				// 值大的不动，小的往后移
				if _, exists := maxList[i]; !exists {
					curNodes[i] = node.Next() //不能用node=node.Next()，因为for range取得的是值拷贝
					// 有指针遍历完，不可能再有新交集
					if curNodes[i] == nil {
						return result
					}
				}
			}
		}
	}
}

// UnionOfSkipList 求多个SkipList的并集
func UnionOfSkipList(lists ...*skiplist.SkipList) *skiplist.SkipList {
	if len(lists) == 0 {
		return nil
	}
	if len(lists) == 1 {
		return lists[0]
	}
	result := skiplist.New(skiplist.Uint64)
	// 用于记录已经添加过的键的集合，防止重复添加
	keySet := make(map[any]struct{}, 1000)
	for _, list := range lists {
		if list == nil {
			continue
		}
		node := list.Front()
		for node != nil {
			if _, exists := keySet[node.Key()]; !exists {
				result.Set(node.Key(), node.Value)
				// 将当前节点的键添加到键集合中，标记为已添加
				keySet[node.Key()] = struct{}{}
			}
			node = node.Next()
		}
	}
	return result
}
