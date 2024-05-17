package kv_db

import (
	"criker-search/internal/kv_db"
	"criker-search/utils"
	"os"
	"strings"
)

// 几种常见的基于LSM-tree算法实现的KV数据库
const (
	BOLT = iota
	BADGER
)

// KeyValueDB k-v数据库接口
type KeyValueDB interface {
	Open() error                                      // 初始化DB
	GetDbPath() string                                // 获取存储数据的目录
	Set(k, v []byte) error                            // 写入<key, value>
	BatchSet(keys, values [][]byte) error             // 批量写入<key, value>
	Get(k []byte) ([]byte, error)                     // 读取key对应的value
	BatchGet(keys [][]byte) ([][]byte, error)         // 批量读取，注意不保证顺序
	Delete(k []byte) error                            // 删除
	BatchDelete(keys [][]byte) error                  // 批量删除
	Has(k []byte) bool                                // 判断某个key是否存在
	IterDB(fn func(k, v []byte) error) (int64, error) // 遍历数据库，返回数据的条数
	IterKey(fn func(k []byte) error) (int64, error)   // 遍历所有key，返回数据的条数
	Close() error                                     // 把内存中的数据flush到磁盘，同时释放文件锁
}

// GetKvDB Factory工厂模式，把类的创建和使用分隔开
// Get函数就是一个工厂，它返回产品的接口，即它可以返回各种各样的具体产品。
func GetKvDB(dbType int, path string) (KeyValueDB, error) {
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

	var db KeyValueDB
	switch dbType {
	case BADGER:
		db = new(kv_db.Badger).WithDataPath(path)
	default:
		// 默认使用Bolt，Builder生成器模式
		db = new(kv_db.Bolt).WithDataPath(path).WithBucket("radic")
	}
	// 创建具体KVDB的细节隐藏在Open()函数里
	err = db.Open()
	return db, err
}
