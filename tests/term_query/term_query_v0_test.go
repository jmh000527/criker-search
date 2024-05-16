package tests

import (
	"fmt"
	"strings"
	"testing"
)

func should(s ...string) string {
	if len(s) == 0 {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString("(")
	for _, ele := range s {
		if len(ele) > 0 {
			sb.WriteString(ele + "|")
		}
	}
	rect := sb.String()
	// 去除最后一个字符串后的“|”
	return rect[:len(rect)-1] + ")"
}

func must(s ...string) string {
	return "(" + strings.Join(s, "&") + ")"
}

// ((((A|B|C)&D)|E)&((F|G)&H))
func TestN(t *testing.T) {
	fmt.Println(must(should(must(should("A", "B", "C"), "D"), "E"), must(should("F", "G"), "H")))
}

func TestTermQueryV0(t *testing.T) {
	A := KeywordExpression("A")
	B := KeywordExpression("B")
	C := KeywordExpression("C")
	D := KeywordExpression("D")
	E := TermQueryV0{} //空Expression
	F := KeywordExpression("F")
	G := KeywordExpression("G")
	H := KeywordExpression("H")

	var exp TermQueryV0

	exp = A
	// print函数会自动调用变量的String()方法
	fmt.Println(exp)

	exp = ShouldExpression(A, B, C)
	fmt.Println(exp)

	// ((A|B|C)&D)|E&((F|G)&H)
	// 函数嵌套的导数太多，编码时需要非常小心
	exp = MustExpression(ShouldExpression(MustExpression(ShouldExpression(A, B, C), D), E), MustExpression(ShouldExpression(F, G), H))
	fmt.Println(exp)
}
