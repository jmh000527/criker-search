package tests

import "strings"

type TermQueryV0 struct {
	Must    []TermQueryV0
	Should  []TermQueryV0
	Keyword string
}

// Empty 判断一个TermQuery是否为空
func (exp TermQueryV0) Empty() bool {
	return len(exp.Keyword) == 0 && len(exp.Must) == 0 && len(exp.Should) == 0
}

// KeywordExpression 将string转换成TermQuery
func KeywordExpression(keyword string) TermQueryV0 {
	return TermQueryV0{
		Keyword: keyword,
	}
}

// MustExpression 若干个TermQuery通过与运算构造出新的TermQuery
func MustExpression(exps ...TermQueryV0) TermQueryV0 {
	if len(exps) == 0 {
		return TermQueryV0{}
	}
	array := make([]TermQueryV0, 0, len(exps))
	// 非空的Expression才能添加到array中
	for _, exp := range exps {
		if !exp.Empty() {
			array = append(array, exp)
		}
	}
	return TermQueryV0{
		Must: array,
	}
}

// ShouldExpression 若干个TermQuery通过或运算构造出新的TermQuery
func ShouldExpression(exps ...TermQueryV0) TermQueryV0 {
	if len(exps) == 0 {
		return TermQueryV0{}
	}
	array := make([]TermQueryV0, 0, len(exps))
	//非空的Expression才能添加到array里面去
	for _, exp := range exps {
		if !exp.Empty() {
			array = append(array, exp)
		}
	}
	return TermQueryV0{
		Should: array,
	}
}

// print函数会自动调用变量的String()方法
func (exp TermQueryV0) String() string {
	// 叶子节点Keyword不为空，本身就是一个TermQuery，直接返回
	if len(exp.Keyword) > 0 {
		return exp.Keyword
	} else if len(exp.Must) > 0 {
		if len(exp.Must) == 1 {
			return exp.Must[0].String()
		} else {
			sb := strings.Builder{}
			sb.WriteString("(")
			for _, exp := range exp.Must {
				sb.WriteString(exp.String())
				sb.WriteString("&")
			}
			s := sb.String()
			s = s[:len(s)-1] + ")"
			return s
		}
	} else if len(exp.Should) > 0 {
		if len(exp.Should) == 1 {
			return exp.Should[0].String()
		} else {
			sb := strings.Builder{}
			sb.WriteString("(")
			for _, exp := range exp.Should {
				sb.WriteString(exp.String())
				sb.WriteString("|")
			}
			s := sb.String()
			s = s[:len(s)-1] + ")"
			return s
		}
	}
	return ""
}
