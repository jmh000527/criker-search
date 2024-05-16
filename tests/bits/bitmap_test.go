package tests

import (
	"fmt"
	"testing"
)

func TestBitMap(T *testing.T) {
	min := 10
	bm1 := CreateBitmap(min, []int{15, 30, 20, 50, 23})
	bm2 := CreateBitmap(min, []int{30, 15, 50, 20, 23, 45})
	fmt.Println(IntersectionOfBitmap(bm1, bm2, min))
}

func TestIntersectionOfTwoOrderedList(t *testing.T) {
	arr1 := []int{15, 20, 23, 30, 50}
	arr2 := []int{12, 15, 23, 30, 45, 50}
	fmt.Println(IntersectionOfTwoOrderedList(arr1, arr2))
}
