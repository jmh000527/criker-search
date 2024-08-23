package kv_db

import (
	"github.com/jmh000527/criker-search/utils"
	"os"
	"strings"
)

// GetKvDB 使用工厂模式创建并返回一个具体的 KeyValueDB 实例。
// 根据指定的数据库类型（dbType）和路径（path），创建并初始化一个 KeyValueDB 的实现。
// 该工厂函数将创建所需的目录结构，并根据 dbType 返回相应的数据库实例。
//
// 参数:
//   - dbType: 数据库类型，表示要创建的数据库的具体类型。
//   - path: 数据库文件路径，指定数据库的数据存储位置。
//
// 返回值:
//   - _interface.KeyValueDB: 创建的 KeyValueDB 实例接口。
//   - error: 如果在创建目录或打开数据库时发生错误，返回相应的错误。
func GetKvDB(dbType int, path string) (KeyValueDB, error) {
	// 分割路径并确定父目录路径
	paths := strings.Split(path, "/")
	parentPath := strings.Join(paths[0:len(paths)-1], "/") // 父路径
	stat, err := os.Stat(parentPath)

	// 如果父路径不存在，则创建它
	if os.IsNotExist(err) {
		utils.Log.Printf("create dir: %s", parentPath)
		if err := os.MkdirAll(parentPath, os.ModePerm); err != nil {
			return nil, err
		}
	} else {
		// 如果父路径存在
		// 如果父路径是普通文件，则删除它
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

	// 根据 dbType 创建相应的 KeyValueDB 实例
	var db KeyValueDB
	switch dbType {
	case BADGER:
		db = new(Badger).WithDataPath(path)
	default:
		// 默认使用 Bolt 数据库，并设置相应的桶
		db = new(Bolt).WithDataPath(path).WithBucket("radic")
	}

	// 创建具体 KVDB 实例的细节被隐藏在 Open() 方法中
	err = db.Open()
	return db, err
}
