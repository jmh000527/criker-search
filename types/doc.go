package types

// ToString 将Word和Field进行拼接，返回拼接后的字符串
func (kw *Keyword) ToString() string {
	if len(kw.Word) > 0 {
		return kw.Word + "\001" + kw.Word
	} else {
		return ""
	}
}
