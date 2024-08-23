package tests

import (
	"fmt"
	"testing"
)

func TestBits(t *testing.T) {
	var n uint64
	n = SetBit1(n, 11)
	n = SetBit1(n, 28)

	fmt.Println(IsBit1(n, 11))
	fmt.Println(IsBit1(n, 28))
	fmt.Println(IsBit1(n, 18))

	fmt.Println(CountBit1(n))

	fmt.Printf("%064b\n", n)
}
