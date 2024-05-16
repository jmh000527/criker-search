package types

import (
	"strings"
)

func NewTermQuery(field, keyword string) *TermQuery {
	return &TermQuery{Keyword: &Keyword{Field: field, Word: keyword}} //TermQuery的一级成员里只有Field-keyword非空，Must和Should都为空
}

func (q *TermQuery) Empty() bool {
	return q.Keyword == nil && len(q.Must) == 0 && len(q.Should) == 0
}

// And Builder模式。方法返回结构体本身
func (q *TermQuery) And(queries ...*TermQuery) *TermQuery {
	if len(queries) == 0 {
		return q
	}
	array := make([]*TermQuery, 0, 1+len(queries))
	//空的query会被排除掉
	if !q.Empty() {
		array = append(array, q)
	}
	for _, ele := range queries {
		if !ele.Empty() {
			array = append(array, ele)
		}
	}
	// 如果所有的 query 都为空，直接返回当前对象 q
	if len(array) == 0 {
		return q
	}
	return &TermQuery{Must: array} //TermQuery的一级成员里只有Must非空，Keyword和Should都为空
}

func (q *TermQuery) Or(queries ...*TermQuery) *TermQuery {
	if len(queries) == 0 {
		return q
	}
	array := make([]*TermQuery, 0, 1+len(queries))
	//空的query会被排除掉
	if !q.Empty() {
		array = append(array, q)
	}
	for _, ele := range queries {
		if !ele.Empty() {
			array = append(array, ele)
		}
	}
	// 如果所有的 query 都为空，直接返回当前对象 q
	if len(array) == 0 {
		return q
	}
	return &TermQuery{Should: array} //TermQuery的一级成员里只有Should非空，Must和Keyword都为空
}

// ToString print函数会自动调用变量的ToString()方法
func (q *TermQuery) ToString() string {
	if q.Keyword != nil {
		return q.Keyword.ToString()
	} else if len(q.Must) > 0 {
		if len(q.Must) == 1 {
			return q.Must[0].ToString()
		} else {
			sb := strings.Builder{}
			sb.WriteByte('(')
			for _, e := range q.Must {
				s := e.ToString()
				if len(s) > 0 {
					sb.WriteString(s)
					sb.WriteByte('&')
				}
			}
			s := sb.String()
			s = s[0:len(s)-1] + ")"
			return s
		}
	} else if len(q.Should) > 0 {
		if len(q.Should) == 1 {
			return q.Should[0].ToString()
		} else {
			sb := strings.Builder{}
			sb.WriteByte('(')
			for _, e := range q.Should {
				s := e.ToString()
				if len(s) > 0 {
					sb.WriteString(s)
					sb.WriteByte('|')
				}
			}
			s := sb.String()
			s = s[0:len(s)-1] + ")"
			return s
		}

	}
	return ""
}
