package types

import (
	"strings"
)

// NewTermQuery 创建并返回一个新的 TermQuery 实例。
// 该实例用于执行基于关键词的查询，只设置了关键词的字段和词语。
// 其它查询条件如 Must 和 Should 都是空的。
//
// 参数:
//   - field: 查询的字段名称，类型为字符串。
//   - keyword: 查询的关键词，类型为字符串。
//
// 返回值:
//   - *TermQuery: 一个新的 TermQuery 实例。
func NewTermQuery(field, keyword string) *TermQuery {
	return &TermQuery{
		// 初始化 TermQuery 的 Keyword 成员，设置字段和关键词。
		Keyword: &Keyword{
			Field: field,
			Word:  keyword,
		},
		// TermQuery 的 Must 和 Should 成员保持为空。
	}
}

// Empty 检查 TermQuery 是否为空。
// 一个 TermQuery 被认为是空的，当且仅当其 Keyword 为 nil，并且 Must 和 Should 列表都为空。
//
// 返回值:
//   - bool: 如果 TermQuery 为空，返回 true；否则返回 false。
func (q *TermQuery) Empty() bool {
	return q.Keyword == nil && len(q.Must) == 0 && len(q.Should) == 0
}

// And 使用 Builder 模式，将多个 TermQuery 进行合并，并返回合并后的 TermQuery。
// 该方法将当前的 TermQuery 与提供的 TermQuery 进行逻辑与（AND）操作。
// 空的 TermQuery 会被排除在外，如果没有有效的查询条件，则返回当前对象 q。
//
// 参数:
//   - queries: 需要与当前 TermQuery 合并的 TermQuery 列表。
//
// 返回值:
//   - *TermQuery: 合并后的 TermQuery 实例，Must 成员包含所有有效的 TermQuery。
func (q *TermQuery) And(queries ...*TermQuery) *TermQuery {
	if len(queries) == 0 {
		return q
	}
	array := make([]*TermQuery, 0, 1+len(queries))
	// 空的 query 会被排除掉
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
	return &TermQuery{Must: array} // TermQuery 的一级成员里只有 Must 非空，Keyword 和 Should 都为空
}

// Or 使用 Builder 模式，将多个 TermQuery 进行合并，并返回合并后的 TermQuery。
// 该方法将当前的 TermQuery 与提供的 TermQuery 进行逻辑或（OR）操作。
// 空的 TermQuery 会被排除在外，如果没有有效的查询条件，则返回当前对象 q。
//
// 参数:
//   - queries: 需要与当前 TermQuery 合并的 TermQuery 列表。
//
// 返回值:
//   - *TermQuery: 合并后的 TermQuery 实例，Should 成员包含所有有效的 TermQuery。
func (q *TermQuery) Or(queries ...*TermQuery) *TermQuery {
	if len(queries) == 0 {
		return q
	}
	array := make([]*TermQuery, 0, 1+len(queries))
	// 空的 query 会被排除掉
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
	return &TermQuery{Should: array} // TermQuery 的一级成员里只有 Should 非空，Must 和 Keyword 都为空
}

// ToString 返回 TermQuery 的字符串表示形式。
// 如果 TermQuery 的 Keyword 成员非空，则返回 Keyword 的字符串表示。
// 如果 TermQuery 的 Must 列表非空，则返回所有 Must 查询的组合表示形式，用逻辑与（&）连接。
// 如果 TermQuery 的 Should 列表非空，则返回所有 Should 查询的组合表示形式，用逻辑或（|）连接。
// 如果 TermQuery 既没有 Keyword，也没有 Must 或 Should 列表，则返回空字符串。
//
// 返回值:
//   - string: TermQuery 的字符串表示形式。
func (q *TermQuery) ToString() string {
	if q.Keyword != nil {
		// 如果 Keyword 非空，直接返回 Keyword 的字符串表示。
		return q.Keyword.ToString()
	} else if len(q.Must) > 0 {
		// 如果 Must 列表非空，构建 Must 查询的字符串表示。
		if len(q.Must) == 1 {
			// 只有一个 Must 查询，直接返回其字符串表示。
			return q.Must[0].ToString()
		} else {
			// 多个 Must 查询，使用逻辑与（&）连接它们。
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
		// 如果 Should 列表非空，构建 Should 查询的字符串表示。
		if len(q.Should) == 1 {
			// 只有一个 Should 查询，直接返回其字符串表示。
			return q.Should[0].ToString()
		} else {
			// 多个 Should 查询，使用逻辑或（|）连接它们。
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
	// 如果 TermQuery 既没有 Keyword 也没有 Must 或 Should 列表，返回空字符串。
	return ""
}
