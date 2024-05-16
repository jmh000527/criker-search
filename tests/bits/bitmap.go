package tests

type Bitmap struct {
	Table uint64
}

func CreateBitmap(min int, arr []int) *Bitmap {
	bitmap := new(Bitmap)
	for _, v := range arr {
		index := v - min
		bitmap.Table = SetBit1(bitmap.Table, index)
	}
	return bitmap
}

// IntersectionOfBitmap 位图求交集
func IntersectionOfBitmap(bm1, bm2 *Bitmap, min int) []int {
	rect := make([]int, 0, 100)
	s := bm1.Table & bm2.Table
	for i := 1; i <= 64; i++ {
		if IsBit1(s, i) {
			rect = append(rect, i+min)
		}
	}
	return rect
}

// IntersectionOfTwoOrderedList 两个有序链表求交集
func IntersectionOfTwoOrderedList(arr1, arr2 []int) []int {
	m := len(arr1)
	n := len(arr2)
	if m == 0 || n == 0 {
		return nil
	}
	rect := make([]int, 0, 100)
	var i, j int
	for i < m && j < n {
		if arr1[i] == arr2[j] {
			// 发现交集
			rect = append(rect, arr1[i])
			i++
			j++
		} else if arr1[i] < arr2[j] {
			i++
		} else {
			j++
		}
	}
	return rect
}
