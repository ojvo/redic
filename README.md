# Redic - Robust Redis Client Wrapper for Go

Redic 是一个基于 `github.com/gomodule/redigo/redis` 封装的增强型 Redis 客户端。它在 Redigo 的基础上增加了自动重连、智能订阅管理、高性能消息分发（Worker Pool）和背压保护等生产级特性。

## 特性 (Features)

*   **自动重连**: 连接断开时自动尝试重连，支持指数退避策略。
*   **智能订阅管理**:
    *   断线重连后自动恢复所有 Channel 和 Pattern 订阅。
    *   使用 Worker Pool 模型分发消息，防止高频消息导致 Goroutine 爆炸。
    *   支持背压保护（Backpressure），提供缓冲区、超时和丢弃回调机制。
*   **高性能**: 优化了内存分配，消除订阅 Key 拼接开销；支持高并发读写。
*   **Redigo 兼容**: 暴露 `Do` 方法，完全兼容 Redigo 的命令执行方式。

## 安装 (Installation)

```bash
go get redic
```

## 快速开始 (Quick Start)

### 1. 创建客户端

```go
package main

import (
    "log"
    "time"
    "redic"
)

func main() {
    // 配置重连策略（可选）
    cfg := redic.DefaultReconnectConfig()
    cfg.MaxRetries = -1 // 无限重试
    
    // 创建客户端
    client := redic.NewClient("127.0.0.1:6379", "", 0, cfg)
    
    // 连接
    if err := client.Connect(); err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer client.Close()
    
    // ... 使用 client
}
```

### 2. 基本命令 (Get/Set/Do)

```go
// 使用封装好的 Set/Get 方法
err := client.Set("my_key", "my_value")
val, err := client.Get("my_key")

// 执行任意 Redis 命令 (兼容 Redigo)
reply, err := client.Do("INCR", "counter")
count, _ := redis.Int(reply, err)
```

### 3. 发布与订阅 (Pub/Sub)

```go
// 订阅频道
err := client.Subscribe("news", func(channel, msg string) {
    log.Printf("Received on %s: %s", channel, msg)
})

// 模式订阅
err := client.PSubscribe("user:*", func(pattern, msg string) {
    log.Printf("Pattern match %s: %s", pattern, msg)
})

// 保持主程序运行
select {}
```

## 迁移指南：从 Redigo 迁移到 Redic (Migration Guide)

Redic 旨在尽可能无缝地替换原有的 Redigo 代码。

### 场景 1: 使用 `redis.Pool`

**原代码 (Redigo):**
```go
pool := &redis.Pool{
    Dial: func() (redis.Conn, error) {
        return redis.Dial("tcp", "localhost:6379")
    },
}

// 使用
conn := pool.Get()
defer conn.Close()
conn.Do("SET", "k", "v")
```

**新代码 (Redic):**
```go
// 初始化 Redic Client (内部管理了 Pool)
client := redic.NewClient("localhost:6379", "", 0, nil)
client.Connect()
defer client.Close()

// 使用: 直接调用 client.Do，无需手动获取/关闭 Conn
client.Do("SET", "k", "v")
```

### 场景 2: 复杂的 Pub/Sub 处理

**原代码 (Redigo):**
需要手动启动 Goroutine，维护 `redis.PubSubConn`，并在循环中 `Receive()`。断线后需要手动重连并重新 Subscribe。

**新代码 (Redic):**
```go
// 只需注册回调，Redic 自动处理重连、重订阅、并发分发
client.Subscribe("channel", func(ch, msg string) {
    // 处理逻辑
})
```

## 高级配置 (Advanced Configuration)

### 性能与背压调优

可以在 `ReconnectConfig` 中调整并发和背压参数，以适应不同的负载场景：

```go
cfg := redic.DefaultReconnectConfig()

// 1. Worker Pool 大小: 控制并发处理回调的 Goroutine 数量
// 默认 0 (自动设置为 CPU 核数 * 4)
cfg.SubscriptionWorkerPoolSize = 100

// 2. 缓冲区大小: 应对突发流量
// 默认 1024
cfg.SubscriptionBufferSize = 10000

// 3. 分发超时: 队列满时的等待时间
// 0 = 立即丢弃; >0 = 等待指定时间
cfg.SubscriptionDispatchTimeout = 100 * time.Millisecond

// 4. 丢弃回调: 监控数据丢失
cfg.OnMessageDropped = func(channel string) {
    log.Printf("Warning: Message dropped on channel %s", channel)
}

client := redic.NewClient(addr, pwd, db, cfg)
```
