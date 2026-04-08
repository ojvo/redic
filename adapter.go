package redic

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// ============================================================================
// Commander — 可自由切换的命令层接口
// ============================================================================

// Commander 定义了纯命令操作接口，Redigo 与 Redic 均可安全实现。
// 业务代码面向 Commander 编程即可实现底层自由切换。
type Commander interface {
	// Connect 建立连接
	Connect() error

	// Close 关闭连接并释放资源
	Close() error

	// Get 获取字符串值
	Get(key string) (string, error)

	// Set 设置值
	Set(key string, value interface{}) error

	// Do 执行任意 Redis 命令
	Do(commandName string, args ...interface{}) (interface{}, error)

	// DoContext 带 context 执行命令
	DoContext(ctx context.Context, commandName string, args ...interface{}) (interface{}, error)

	// Publish 发布消息
	Publish(channel string, message interface{}) error
}

// ============================================================================
// Subscriber — Redic 专有的订阅接口
// ============================================================================

// Subscriber 定义了 Pub/Sub 订阅操作，这是 Redic 专有能力。
// 由于 Redigo 原生 PubSub 模型（阻塞 Receive 循环）与回调模型不兼容，
// 此接口不试图统一两者，而是显式声明为 Redic 的增值能力。
type Subscriber interface {
	// Subscribe 回调模式订阅（handler 在 worker pool 中执行）
	Subscribe(channel string, handler func(ch, msg string)) error

	// PSubscribe 回调模式的模式订阅
	PSubscribe(pattern string, handler func(ch, msg string)) error

	// SubscribeWithOptions 带选项的订阅（支持 Ordered 保序模式）
	SubscribeWithOptions(channel string, handler func(ch, msg string), opts SubscribeOptions) error

	// PSubscribeWithOptions 带选项的模式订阅
	PSubscribeWithOptions(pattern string, handler func(ch, msg string), opts SubscribeOptions) error

	// Unsubscribe 取消订阅
	Unsubscribe(channels ...string) error

	// PUnsubscribe 取消模式订阅
	PUnsubscribe(patterns ...string) error
}

// ============================================================================
// Adapter = Commander + Subscriber + 可观测性（向后兼容）
// ============================================================================

// Adapter 组合了 Commander 与 Subscriber，并附加可观测能力。
// 保留此接口以兼容已有代码——新代码建议直接使用 Commander 或 Subscriber。
type Adapter interface {
	Commander
	Subscriber

	// GetState 获取连接状态（Redic 专有）
	GetState() ConnectionState

	// GetMetrics 获取运行指标（Redic 专有）
	GetMetrics() Metrics
}

// ============================================================================
// redicCommander — Redic Client 实现 Commander
// ============================================================================

type redicCommander struct {
	client *Client
}

// NewRedicCommander 从连接参数创建基于 Redic 的 Commander
func NewRedicCommander(addr, password string, db int, cfg *ReconnectConfig) Commander {
	return &redicCommander{client: NewClient(addr, password, db, cfg)}
}

// NewRedicCommanderFromClient 从已有 Client 创建 Commander
func NewRedicCommanderFromClient(c *Client) Commander {
	return &redicCommander{client: c}
}

func (r *redicCommander) Connect() error { return r.client.Connect() }
func (r *redicCommander) Close() error   { return r.client.Close() }
func (r *redicCommander) Get(key string) (string, error) {
	return r.client.Get(key)
}
func (r *redicCommander) Set(key string, value interface{}) error {
	return r.client.Set(key, value)
}
func (r *redicCommander) Do(cmd string, args ...interface{}) (interface{}, error) {
	return r.client.Do(cmd, args...)
}
func (r *redicCommander) DoContext(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	return r.client.DoContext(ctx, cmd, args...)
}
func (r *redicCommander) Publish(ch string, msg interface{}) error {
	return r.client.Publish(ch, msg)
}

// ============================================================================
// redigoCommander — 原生 Redigo Pool 实现 Commander
// ============================================================================

// RedigoConfig 创建 redigoCommander 所需的配置
type RedigoConfig struct {
	Addr         string
	Password     string
	DB           int
	MaxIdle      int
	MaxActive    int
	IdleTimeout  time.Duration
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// DefaultRedigoConfig 返回默认 Redigo 配置
func DefaultRedigoConfig(addr, password string, db int) *RedigoConfig {
	return &RedigoConfig{
		Addr:         addr,
		Password:     password,
		DB:           db,
		MaxIdle:      10,
		MaxActive:    100,
		IdleTimeout:  240 * time.Second,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
}

type redigoCommander struct {
	pool *redis.Pool
	cfg  *RedigoConfig
}

// NewRedigoCommander 从配置创建基于原生 Redigo 的 Commander
func NewRedigoCommander(cfg *RedigoConfig) Commander {
	pool := &redis.Pool{
		MaxIdle:     cfg.MaxIdle,
		MaxActive:   cfg.MaxActive,
		Wait:        true,
		IdleTimeout: cfg.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			opts := []redis.DialOption{
				redis.DialConnectTimeout(cfg.DialTimeout),
				redis.DialReadTimeout(cfg.ReadTimeout),
				redis.DialWriteTimeout(cfg.WriteTimeout),
				redis.DialDatabase(cfg.DB),
			}
			if cfg.Password != "" {
				opts = append(opts, redis.DialPassword(cfg.Password))
			}
			return redis.Dial("tcp", cfg.Addr, opts...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
	return &redigoCommander{pool: pool, cfg: cfg}
}

func (r *redigoCommander) Connect() error {
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("PING")
	return err
}

func (r *redigoCommander) Close() error {
	return r.pool.Close()
}

func (r *redigoCommander) Get(key string) (string, error) {
	conn := r.pool.Get()
	defer conn.Close()
	return redis.String(conn.Do("GET", key))
}

func (r *redigoCommander) Set(key string, value interface{}) error {
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", key, value)
	return err
}

func (r *redigoCommander) Do(commandName string, args ...interface{}) (interface{}, error) {
	conn := r.pool.Get()
	defer conn.Close()
	return conn.Do(commandName, args...)
}

func (r *redigoCommander) DoContext(ctx context.Context, commandName string, args ...interface{}) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	conn := r.pool.Get()
	defer conn.Close()
	if dc, ok := conn.(redis.ConnWithContext); ok {
		return dc.DoContext(ctx, commandName, args...)
	}
	return conn.Do(commandName, args...)
}

func (r *redigoCommander) Publish(channel string, message interface{}) error {
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("PUBLISH", channel, message)
	return err
}

// String 方便调试
func (r *redigoCommander) String() string {
	return fmt.Sprintf("redigoCommander{addr=%s, db=%d}", r.cfg.Addr, r.cfg.DB)
}

// ============================================================================
// redicAdapter — 完整的 Adapter 实现（向后兼容）
// ============================================================================

type redicAdapter struct {
	client *Client
}

// NewAdapter 创建基于 Redic Client 的完整适配器
func NewAdapter(addr, password string, db int, cfg *ReconnectConfig) Adapter {
	return &redicAdapter{client: NewClient(addr, password, db, cfg)}
}

// NewAdapterFromClient 从已有 Client 创建适配器
func NewAdapterFromClient(c *Client) Adapter {
	return &redicAdapter{client: c}
}

func (a *redicAdapter) Connect() error                          { return a.client.Connect() }
func (a *redicAdapter) Close() error                            { return a.client.Close() }
func (a *redicAdapter) Get(key string) (string, error)          { return a.client.Get(key) }
func (a *redicAdapter) Set(key string, value interface{}) error { return a.client.Set(key, value) }
func (a *redicAdapter) Do(cmd string, args ...interface{}) (interface{}, error) {
	return a.client.Do(cmd, args...)
}
func (a *redicAdapter) DoContext(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	return a.client.DoContext(ctx, cmd, args...)
}
func (a *redicAdapter) Publish(ch string, msg interface{}) error { return a.client.Publish(ch, msg) }
func (a *redicAdapter) Subscribe(ch string, h func(string, string)) error {
	return a.client.Subscribe(ch, h)
}
func (a *redicAdapter) PSubscribe(p string, h func(string, string)) error {
	return a.client.PSubscribe(p, h)
}
func (a *redicAdapter) SubscribeWithOptions(ch string, h func(string, string), opts SubscribeOptions) error {
	return a.client.SubscribeWithOptions(ch, h, opts)
}
func (a *redicAdapter) PSubscribeWithOptions(p string, h func(string, string), opts SubscribeOptions) error {
	return a.client.PSubscribeWithOptions(p, h, opts)
}
func (a *redicAdapter) Unsubscribe(chs ...string) error { return a.client.Unsubscribe(chs...) }
func (a *redicAdapter) PUnsubscribe(ps ...string) error { return a.client.PUnsubscribe(ps...) }
func (a *redicAdapter) GetState() ConnectionState       { return a.client.GetState() }
func (a *redicAdapter) GetMetrics() Metrics             { return a.client.GetMetrics() }
