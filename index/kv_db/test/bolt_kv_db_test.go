package test

import (
	"github.com/jmh000527/criker-search/index/kv_db"
	"github.com/jmh000527/criker-search/utils"
	"testing"
)

func TestBolt(t *testing.T) {
	setup = func() {
		var err error
		db, err = kv_db.GetKvDB(kv_db.BOLT, utils.RootPath+"data/bolt_db") //使用工厂模式
		if err != nil {
			panic(err)
		}
	}

	// 子测试
	t.Run("bolt_test", testPipeline)
}
