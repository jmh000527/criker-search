package demo

import "golang.org/x/exp/slices"

// 视频类别枚举
const (
	ZiXun    = 1 << iota // 1 << 0
	SheHui               // 1 << 1
	ReDian               // 1 << 2
	ShengHuo             // 1 << 3
	ZhiShi
	HuanQiu
	YouXi
	ZongHe
	RiChang
	YingShi
	DongHua
	KeJi
	YuLe
	BianCheng
)

// GetClassBits 从Keywords中提取类型，用bits表示类别
func GetClassBits(keywords []string) uint64 {
	var bits uint64
	if slices.Contains(keywords, "资讯") {
		bits |= ZiXun //属于哪个类别，就把对应的bit置为1。可能属于多个类别
	}
	if slices.Contains(keywords, "社会") {
		bits |= SheHui
	}
	if slices.Contains(keywords, "热点") {
		bits |= ReDian
	}
	if slices.Contains(keywords, "生活") {
		bits |= ShengHuo
	}
	if slices.Contains(keywords, "知识") {
		bits |= ZhiShi
	}
	if slices.Contains(keywords, "环球") {
		bits |= HuanQiu
	}
	if slices.Contains(keywords, "游戏") {
		bits |= YouXi
	}
	if slices.Contains(keywords, "综合") {
		bits |= ZongHe
	}
	if slices.Contains(keywords, "日常") {
		bits |= RiChang
	}
	if slices.Contains(keywords, "影视") {
		bits |= YingShi
	}
	if slices.Contains(keywords, "科技") {
		bits |= KeJi
	}
	if slices.Contains(keywords, "编程") {
		bits |= BianCheng
	}
	return bits
}
