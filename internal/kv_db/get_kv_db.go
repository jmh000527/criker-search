package kv_db

import (
	_interface "criker-search/internal/interface"
	"criker-search/utils"
	"os"
	"strings"
)

// GetKvDB Factory工厂模式，把类的创建和使用分隔开
// Get函数就是一个工厂，它返回产品的接口，即它可以返回各种各样的具体产品。
func GetKvDB(dbType int, path string) (_interface.KeyValueDB, error) {
	paths := strings.Split(path, "/")
	parentPath := strings.Join(paths[0:len(paths)-1], "/") //父路径
	stat, err := os.Stat(parentPath)
	// 若父路径不存在则创建
	if os.IsNotExist(err) {
		utils.Log.Printf("create dir: %s", parentPath)
		// 创建目录
		if err := os.MkdirAll(parentPath, os.ModePerm); err != nil {
			return nil, err
		}
	} else {
		// 父路径存在
		// 如果父路径是个普通文件，则把它删掉
		if stat.Mode().IsRegular() {
			utils.Log.Printf("%s is a regular file, will delete it", parentPath)
			if err := os.Remove(parentPath); err != nil {
				return nil, err
			}
		}
		// 重新创建目录
		if err := os.MkdirAll(parentPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	var db _interface.KeyValueDB
	switch dbType {
	case _interface.BADGER:
		db = new(Badger).WithDataPath(path)
	default:
		// 默认使用Bolt，Builder生成器模式
		db = new(Bolt).WithDataPath(path).WithBucket("radic")
	}
	// 创建具体KVDB的细节隐藏在Open()函数里
	err = db.Open()
	return db, err
}
