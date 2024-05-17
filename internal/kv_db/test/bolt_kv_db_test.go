package test

import (
	_interface "criker-search/internal/interface"
	"criker-search/internal/kv_db"
	"criker-search/utils"
	"testing"
)

func TestBolt(t *testing.T) {
	setup = func() {
		var err error
		db, err = kv_db.GetKvDB(_interface.BOLT, utils.RootPath+"data/bolt_db") //使用工厂模式
		if err != nil {
			panic(err)
		}
	}

	// 子测试
	t.Run("bolt_test", testPipeline)
}
