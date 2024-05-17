package utils

import (
	"path"
	"runtime"
)

var (
	RootPath string //项目根目录
)

func init() {
	RootPath = path.Dir(GetCurrentPath()+"..") + "/" //项目根目录
}

// GetCurrentPath 获取当前函数所在go代码的路径
func GetCurrentPath() string {
	_, filename, _, _ := runtime.Caller(1) //1表示当前函数，2表示调用本函数的函数，3...依次类推
	return path.Dir(filename)
}
