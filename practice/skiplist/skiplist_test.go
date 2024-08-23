package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/huandu/skiplist"
)

func TestSkipList(t *testing.T) {
	list := skiplist.New(skiplist.Int32)
	list.Set(24, 31) //skiplist是一个按key排序好的map
	list.Set(24, 40) //相同的key, value会覆盖前值
	list.Set(12, 40) //添加元素
	list.Set(18, 3)
	list.Remove(12)                         //删除元素
	if value, ok := list.GetValue(18); ok { //查找key对应的value
		fmt.Println(value)
	}
	//遍历。自动按key排好序
	fmt.Println("------------------")
	node := list.Front()
	for node != nil {
		fmt.Println(node.Key(), node.Value)
		node = node.Next() //迭代器模式
	}
}

func TestIntersectionOfSkipList(t *testing.T) {
	l1 := skiplist.New(skiplist.Uint64)
	l1.Set(uint64(5), 0)
	l1.Set(uint64(1), 0)
	l1.Set(uint64(4), 0)
	l1.Set(uint64(9), 0)
	l1.Set(uint64(11), 0)
	l1.Set(uint64(7), 0)
	//skiplist内部会自动做排序，排完序之后为 1 4 5 7 9 11

	l2 := skiplist.New(skiplist.Uint64)
	l2.Set(uint64(4), 0)
	l2.Set(uint64(5), 0)
	l2.Set(uint64(9), 0)
	l2.Set(uint64(8), 0)
	l2.Set(uint64(2), 0)
	//skiplist内部会自动做排序，排完序之后为 2 4 5 8 9

	l3 := skiplist.New(skiplist.Uint64)
	l3.Set(uint64(3), 0)
	l3.Set(uint64(5), 0)
	l3.Set(uint64(7), 0)
	l3.Set(uint64(9), 0)
	//skiplist内部会自动做排序，排完序之后为 3 5 7 9

	interset := IntersectionOfSkipLists()
	if interset != nil {
		node := interset.Front()
		for node != nil {
			fmt.Printf("%d ", node.Key().(uint64))
			node = node.Next()
		}
	}
	fmt.Println("\n" + strings.Repeat("-", 50))

	interset = IntersectionOfSkipLists(l1)
	if interset != nil {
		node := interset.Front()
		for node != nil {
			fmt.Printf("%d ", node.Key().(uint64))
			node = node.Next()
		}
	}
	fmt.Println("\n" + strings.Repeat("-", 50))

	interset = IntersectionOfSkipLists(l1, l2)
	if interset != nil {
		node := interset.Front()
		for node != nil {
			fmt.Printf("%d ", node.Key().(uint64))
			node = node.Next()
		}
	}
	fmt.Println("\n" + strings.Repeat("-", 50))

	interset = IntersectionOfSkipLists(l1, l2, l3)
	if interset != nil {
		node := interset.Front()
		for node != nil {
			fmt.Printf("%d ", node.Key().(uint64))
			node = node.Next()
		}
	}
	fmt.Println("\n" + strings.Repeat("-", 50))

	union := UnionOfSkipList()
	if union != nil {
		node := union.Front()
		for node != nil {
			fmt.Printf("%d ", node.Key().(uint64))
			node = node.Next()
		}
	}
	fmt.Println("\n" + strings.Repeat("-", 50))

	union = UnionOfSkipList(l1)
	if union != nil {
		node := union.Front()
		for node != nil {
			fmt.Printf("%d ", node.Key().(uint64))
			node = node.Next()
		}
	}
	fmt.Println("\n" + strings.Repeat("-", 50))

	union = UnionOfSkipList(l1, l2)
	if union != nil {
		node := union.Front()
		for node != nil {
			fmt.Printf("%d ", node.Key().(uint64))
			node = node.Next()
		}
	}
	fmt.Println("\n" + strings.Repeat("-", 50))

	union = UnionOfSkipList(l1, l2, l3)
	if union != nil {
		node := union.Front()
		for node != nil {
			fmt.Printf("%d ", node.Key().(uint64))
			node = node.Next()
		}
	}
	fmt.Println("\n" + strings.Repeat("-", 50))
}
