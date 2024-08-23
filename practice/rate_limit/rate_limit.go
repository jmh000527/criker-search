package tests

import (
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// TotalQuery 记录请求总数
var TotalQuery int32

// Handler 模拟接口
func Handler() {
	atomic.AddInt32(&TotalQuery, 1)
	time.Sleep(50 * time.Millisecond)
}

func CallHandler() {
	// 每隔100ms生成一个令牌，最大QPS限制为10
	limiter := rate.NewLimiter(rate.Every(100*time.Millisecond), 10)
	for {
		// 方式一：WaitN()
		//ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		//defer cancel()
		//limiter.WaitN(ctx, 1) // 阻塞，直到桶中有N个令牌。N=1时等价于Wait(ctx)
		//Handler()

		// 方式二：AllowN()
		// 当前桶中是否至少还有N个令牌，如果有则返回true。N=1时等价于Allow(time.Time)
		//if limiter.AllowN(time.Now(), 1) {
		//	Handler()
		//}

		// 方式三：ReserveN()
		// reserve.Delay()告诉你还需要等多久才会有充足的令牌，等待相应时间，再执行
		reserve := limiter.ReserveN(time.Now(), 1)
		time.Sleep(reserve.Delay())
		Handler()
	}
}
