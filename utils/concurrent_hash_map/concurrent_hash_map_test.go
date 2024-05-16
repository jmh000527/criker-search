package utils

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

var conMap = NewConcurrentHashMap(8, 1000)
var synMap = sync.Map{}

func readConMap() {
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(int(rand.Int63()))
		conMap.Get(key)
	}
}

func writeConMap() {
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(int(rand.Int63()))
		conMap.Set(key, 1)
	}
}

func readSynMap() {
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(int(rand.Int63()))
		synMap.Load(key)
	}
}

func writeSynMap() {
	for i := 0; i < 10000; i++ {
		key := strconv.Itoa(int(rand.Int63()))
		synMap.Store(key, 1)
	}
}

func BenchmarkConMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		const P = 600
		wg := sync.WaitGroup{}
		wg.Add(2 * P)
		// 600个协程一直读
		for j := 0; j < P; j++ {
			go func() {
				defer wg.Done()
				readConMap()
			}()
		}
		// 600个协程一直写
		for j := 0; j < P; j++ {
			go func() {
				defer wg.Done()
				writeConMap()
				//time.Sleep(100 * time.Millisecond)
			}()
		}
		wg.Wait()
	}
}

func BenchmarkSynMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		const P = 600
		wg := sync.WaitGroup{}
		wg.Add(2 * P)
		// 600个协程一直读
		for j := 0; j < P; j++ {
			go func() {
				defer wg.Done()
				readSynMap()
			}()
		}
		// 600个协程一直写
		for j := 0; j < P; j++ {
			go func() {
				defer wg.Done()
				writeSynMap()
				//time.Sleep(100 * time.Millisecond)
			}()
		}
		wg.Wait()
	}
}
