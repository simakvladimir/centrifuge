package bench

import (
	"fmt"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
)

const redisStandaloneAddress = "localhost:6379"

var rawData = []byte(`{"bench": true}`)

func Benchmark_RedisPublish_FlushDelay100ms(b *testing.B) {
	runBench(b, 0, 100*time.Microsecond)
}

func Benchmark_RedisPublish_FlushDelay0ms(b *testing.B) {
	runBench(b, 0, -1*time.Microsecond)
}

func Benchmark_RedisPublishParallel_FlushDelay100ms(b *testing.B) {
	runBench(b, 1, 100*time.Microsecond)
}

func Benchmark_RedisPublishParallel_FlushDelay0ms(b *testing.B) {
	runBench(b, 1, -1*time.Microsecond)
}

func Benchmark_RedisPublishParallel10_FlushDelay100ms(b *testing.B) {
	runBench(b, 10, 100*time.Microsecond)
}

func Benchmark_RedisPublishParallel10_FlushDelay0ms(b *testing.B) {
	runBench(b, 10, -1*time.Microsecond)
}

func newNode(maxFlushDelay time.Duration) *centrifuge.Node {
	node, err := centrifuge.New(centrifuge.Config{
		LogLevel: centrifuge.LogLevelError,
	})
	if err != nil {
		panic(err)
	}

	shard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{
		Address:       redisStandaloneAddress,
		MaxFlushDelay: maxFlushDelay,
	})
	if err != nil {
		panic(err)
	}

	broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
		Shards: []*centrifuge.RedisShard{shard},
	})
	if err != nil {
		panic(err)
	}

	node.SetBroker(broker)

	presenceManager, err := centrifuge.NewRedisPresenceManager(node, centrifuge.RedisPresenceManagerConfig{
		Shards: []*centrifuge.RedisShard{shard},
	})
	if err != nil {
		panic(err)
	}

	node.SetPresenceManager(presenceManager)

	return node
}

func runBench(b *testing.B, parallelism int, flushDelay time.Duration) {
	node := newNode(flushDelay)
	if parallelism < 1 {
		for b.Loop() {
			publish(node)
		}

		return
	}

	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			publish(node)
		}
	})
}

func publish(node *centrifuge.Node) {
	for i := range 100 {
		_, err := node.Publish(fmt.Sprintf("channel%d", i), rawData)
		if err != nil {
			panic(err)
		}
	}
}
