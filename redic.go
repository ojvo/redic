package redic

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gomodule/redigo/redis"
)

// --------------------------------------------------------------------------------
// 错误定义与常量
// --------------------------------------------------------------------------------

var nonNetworkErrors = []error{
	redis.ErrNil,
	context.Canceled,
	context.DeadlineExceeded,
}

var knownNetworkErrors = []error{
	io.EOF,
	io.ErrUnexpectedEOF,
	io.ErrClosedPipe,
	os.ErrDeadlineExceeded,
	redis.ErrPoolExhausted,
}

// 常见网络层系统错误，用于底层判断
var networkSyscallErrnos = map[syscall.Errno]struct{}{
	syscall.ECONNRESET:   {},
	syscall.ECONNABORTED: {},
	syscall.ECONNREFUSED: {},
	syscall.EPIPE:        {},
	syscall.ETIMEDOUT:    {},
	syscall.ENETDOWN:     {},
	syscall.ENETUNREACH:  {},
	syscall.ENETRESET:    {},
	syscall.ENOTCONN:     {},
	syscall.ESHUTDOWN:    {},
	syscall.EHOSTDOWN:    {},
	syscall.EHOSTUNREACH: {},
}

const (
	defaultInitialDelay      = 1 * time.Second
	defaultMaxDelay          = 30 * time.Second
	defaultBackoffMultiplier = 2.0
	defaultPingTimeout       = 5 * time.Second
	defaultReadDeadline      = 60 * time.Second // 读超时（心跳检测）
)

// 连接状态
type ConnectionState int32

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
	StateReconnecting
	StateFailed
)

func (s ConnectionState) String() string {
	states := []string{"Disconnected", "Connecting", "Connected", "Reconnecting", "Failed"}
	if int(s) < len(states) {
		return states[s]
	}
	return "Unknown"
}

// --------------------------------------------------------------------------------
// 配置结构体
// --------------------------------------------------------------------------------

type ReconnectConfig struct {
	MaxRetries        int           // -1 表示无限重试
	InitialDelay      time.Duration
	MaxDelay          time.Duration
	BackoffMultiplier float64
	Jitter            bool
	PingTimeout       time.Duration

	// 事件回调
	OnConnecting   func()
	OnConnected    func()
	OnDisconnected func(error)
	OnReconnecting func(attempt int)
	OnReconnected  func(attempt int)
	OnGiveUp       func(error)
}

func DefaultReconnectConfig() *ReconnectConfig {
	return &ReconnectConfig{
		MaxRetries:        -1, // 无限重试
		InitialDelay:      defaultInitialDelay,
		MaxDelay:          defaultMaxDelay,
		BackoffMultiplier: defaultBackoffMultiplier,
		Jitter:            true,
		PingTimeout:       defaultPingTimeout,
	}
}

// --------------------------------------------------------------------------------
// 订阅信息实体
// --------------------------------------------------------------------------------

type SubscriptionInfo struct {
	Channel   string
	Pattern   string
	Handler   func(channel, message string)
	IsPattern bool
	Active    int32 // atomic: 1=active, 0=inactive
}

func (s *SubscriptionInfo) Key() string {
	if s.IsPattern {
		return "pattern:" + s.Pattern
	}
	return "channel:" + s.Channel
}

func (s *SubscriptionInfo) IsActive() bool {
	return atomic.LoadInt32(&s.Active) == 1
}

// --------------------------------------------------------------------------------
// 订阅管理器
// --------------------------------------------------------------------------------

type SubscriptionManager struct {
	// 保护 subscriptions map
	subMu         sync.RWMutex
	subscriptions map[string]*SubscriptionInfo

	// 保护 pubsubConn 对象本身指针的读写
	connMu     sync.RWMutex
	pubsubConn *redis.PubSubConn

	pool   *redis.Pool
	client *Client
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 状态标志，确保只启动一个接收协程
	receiving int32
}

func NewSubscriptionManager(pool *redis.Pool, client *Client) *SubscriptionManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &SubscriptionManager{
		subscriptions: make(map[string]*SubscriptionInfo),
		pool:          pool,
		ctx:           ctx,
		cancel:        cancel,
		client:        client,
	}
}

// ensureConnection 确保连接已建立并启动接收循环
func (sm *SubscriptionManager) ensureConnection() (*redis.PubSubConn, error) {
	sm.connMu.Lock()
	defer sm.connMu.Unlock()

	// 1. 如果已有连接，直接返回
	if sm.pubsubConn != nil {
		return sm.pubsubConn, nil
	}

	// 2. 检查 Client 状态，如果主 Client 断开了，直接拒绝新的连接尝试
	if sm.client.GetState() != StateConnected {
		return nil, errors.New("client must be in connected state to create subscription connection")
	}

	// 3. 创建新连接
	conn := sm.pool.Get()
	// 测试连接是否有效
	if _, err := conn.Do("PING"); err != nil {
		conn.Close()
		return nil, err
	}

	psc := &redis.PubSubConn{Conn: conn}
	sm.pubsubConn = psc

	// 4. 启动接收协程 (保证只启动一次)
	if atomic.CompareAndSwapInt32(&sm.receiving, 0, 1) {
		sm.wg.Add(1)
		go sm.subscriptionReceiver()
	}

	return psc, nil
}

func (sm *SubscriptionManager) Subscribe(channel string, handler func(channel, message string)) error {
	if handler == nil {
		return errors.New("handler cannot be nil")
	}

	sub := &SubscriptionInfo{
		Channel:   channel,
		Handler:   handler,
		IsPattern: false,
		Active:    1,
	}

	sm.subMu.Lock()
	sm.subscriptions[sub.Key()] = sub
	sm.subMu.Unlock()

	return sm.doSubscribe(sub)
}

func (sm *SubscriptionManager) PSubscribe(pattern string, handler func(channel, message string)) error {
	if handler == nil {
		return errors.New("handler cannot be nil")
	}

	sub := &SubscriptionInfo{
		Pattern:   pattern,
		Handler:   handler,
		IsPattern: true,
		Active:    1,
	}

	sm.subMu.Lock()
	sm.subscriptions[sub.Key()] = sub
	sm.subMu.Unlock()

	return sm.doSubscribe(sub)
}

// 执行底层订阅操作
func (sm *SubscriptionManager) doSubscribe(subscription *SubscriptionInfo) error {
	psc, err := sm.ensureConnection()
	if err != nil {
		// 如果连接尚未准备好，只保存到 map 中，等待重连逻辑处理
		// 并不是严重错误，只要不是逻辑错误都可以接受静默失败
		log.Printf("[Redic] 暂时无法发送订阅命令（将在重连时自动恢复）: %v", err)
		return nil
	}

	if subscription.IsPattern {
		err = psc.PSubscribe(subscription.Pattern)
	} else {
		err = psc.Subscribe(subscription.Channel)
	}

	return err
}

func (sm *SubscriptionManager) Unsubscribe(channels ...string) error {
	// 1. 先清理 Map，如果在发送命令前断网，这能保证重连后不会再订阅这些频道
	sm.subMu.Lock()
	for _, ch := range channels {
		delete(sm.subscriptions, "channel:"+ch)
	}
	sm.subMu.Unlock()

	// 2. 获取连接操作
	sm.connMu.RLock()
	psc := sm.pubsubConn
	sm.connMu.RUnlock()

	if psc != nil {
		args := make([]interface{}, len(channels))
		for i, v := range channels {
			args[i] = v
		}
		return psc.Unsubscribe(args...)
	}
	return nil
}

func (sm *SubscriptionManager) PUnsubscribe(patterns ...string) error {
	sm.subMu.Lock()
	for _, p := range patterns {
		delete(sm.subscriptions, "pattern:"+p)
	}
	sm.subMu.Unlock()

	sm.connMu.RLock()
	psc := sm.pubsubConn
	sm.connMu.RUnlock()

	if psc != nil {
		args := make([]interface{}, len(patterns))
		for i, v := range patterns {
			args[i] = v
		}
		return psc.PUnsubscribe(args...)
	}
	return nil
}

// --------------------------------------------------------------------------------
// 核心接收与重连逻辑
// --------------------------------------------------------------------------------

func (sm *SubscriptionManager) subscriptionReceiver() {
	defer sm.wg.Done()
	defer atomic.StoreInt32(&sm.receiving, 0)

	for {
		// 1. 检查退出
		select {
		case <-sm.ctx.Done():
			return
		default:
		}

		// 2. [修复死锁关键点] 获取连接后立即释放 RLock，不允许带着锁进入阻塞 IO
		sm.connMu.RLock()
		psc := sm.pubsubConn
		sm.connMu.RUnlock()

		if psc == nil {
			// 连接为空，说明可能正在断线/重连中，稍等重试
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// 3. 设置心跳/读超时
		if conn, ok := psc.Conn.(interface{ SetReadDeadline(time.Time) error }); ok {
			conn.SetReadDeadline(time.Now().Add(defaultReadDeadline))
		}

		// 4. 阻塞接收 (IO Wait)
		msg := psc.Receive()

		// 5. 处理结果
		if err, ok := msg.(error); ok {
			// 处理超时错误
			if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
				// 仅是心跳超时，连接正常，继续循环
				continue
			}

			// 如果是其他错误，且 Context 未取消，认为是真正断线
			if sm.ctx.Err() == nil {
				log.Printf("[Redic] 订阅连接断开: %v", err)
				// 异步触发，避免阻塞自身
				go sm.handleDisconnect()
			}
			return
		}

		sm.handleMessage(msg)
	}
}

// 处理断线清理
func (sm *SubscriptionManager) handleDisconnect() {
	sm.connMu.Lock()
	if sm.pubsubConn != nil {
		sm.pubsubConn.Close()
		sm.pubsubConn = nil
	}
	sm.connMu.Unlock()

	// 触发 Client 级别的重连
	if sm.client != nil {
		sm.client.triggerReconnect(errors.New("subscription connection lost"))
	}
}

// 重新订阅所有 (重连成功后调用)
func (sm *SubscriptionManager) ResubscribeAll() {
	log.Println("[Redic] 开始恢复订阅...")

	// 1. 确保新连接建立
	_, err := sm.ensureConnection()
	if err != nil {
		log.Printf("[Redic] 恢复订阅失败，无法建立连接: %v", err)
		return
	}

	// 2. 获取当前所有活跃订阅快照
	sm.subMu.RLock()
	var subs []*SubscriptionInfo
	for _, sub := range sm.subscriptions {
		if sub.IsActive() {
			subs = append(subs, sub)
		}
	}
	sm.subMu.RUnlock()

	// 3. 逐个重新发送指令
	count := 0
	for _, sub := range subs {
		// [修复竞态条件关键点]
		// 在快照之后、实际发送订阅前，需再次确认该订阅是否仍存在于 Map 中
		// 防止断网期间用户调用 Unsubscribe，但重连逻辑错误地又把它恢复了
		sm.subMu.RLock()
		_, exists := sm.subscriptions[sub.Key()]
		sm.subMu.RUnlock()

		if !exists {
			continue // 已经被删除了，跳过
		}

		if err := sm.doSubscribe(sub); err != nil {
			log.Printf("[Redic] 恢复订阅 %s 失败: %v", sub.Key(), err)
		} else {
			count++
		}
	}
	log.Printf("[Redic] 订阅恢复完成 (成功: %d / 总数: %d)", count, len(subs))
}

func (sm *SubscriptionManager) handleMessage(msg interface{}) {
	switch v := msg.(type) {
	case redis.Message:
		// Redigo 中 redis.Message 结构体同时处理普通消息和模式消息
		// 如果 Pattern 字段不为空，则是 PMessage
		if v.Pattern != "" {
			sm.dispatchPatternMessage(v)
		} else {
			sm.dispatch(v.Channel, string(v.Data), false)
		}
	case redis.Subscription:
		// 订阅/退订成功通知，一般忽略
	case redis.Pong:
		// 心跳响应，忽略
	}
}

func (sm *SubscriptionManager) dispatch(key string, data string, isPattern bool) {
	lookupKey := "channel:" + key
	if isPattern {
		lookupKey = "pattern:" + key
	}

	sm.subMu.RLock()
	sub, ok := sm.subscriptions[lookupKey]
	sm.subMu.RUnlock()

	if ok && sub.IsActive() && sub.Handler != nil {
		// 异步执行 Handler，防止用户逻辑阻塞接收循环
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[Redic] Handler panic for %s: %v", key, r)
				}
			}()
			sub.Handler(key, data)
		}()
	}
}

func (sm *SubscriptionManager) dispatchPatternMessage(msg redis.Message) {
	lookupKey := "pattern:" + msg.Pattern

	sm.subMu.RLock()
	sub, ok := sm.subscriptions[lookupKey]
	sm.subMu.RUnlock()

	if ok && sub.IsActive() && sub.Handler != nil {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[Redic] (Pattern) Handler panic: %v", r)
				}
			}()
			// 传回实际 Channel (msg.Channel) 和数据
			sub.Handler(msg.Channel, string(msg.Data))
		}()
	}
}

func (sm *SubscriptionManager) Close() {
	sm.cancel() // 停止接收循环 Context

	// 获取写锁关闭连接
	// 因为 Receiver 已释放读锁并在网络 IO 上等待，这里能获取到锁并 Close
	sm.connMu.Lock()
	if sm.pubsubConn != nil {
		sm.pubsubConn.Close() // 这会立即使 Receive 返回错误，协程退出
		sm.pubsubConn = nil
	}
	sm.connMu.Unlock()

	// 清空本地记录
	sm.subMu.Lock()
	sm.subscriptions = make(map[string]*SubscriptionInfo)
	sm.subMu.Unlock()

	sm.wg.Wait() // 等待接收协程彻底退出
}

func (sm *SubscriptionManager) GetSubscriptionCount() int {
	sm.subMu.RLock()
	defer sm.subMu.RUnlock()
	return len(sm.subscriptions)
}

// --------------------------------------------------------------------------------
// Client (主客户端)
// --------------------------------------------------------------------------------

type Client struct {
	addr            string
	password        string
	database        int
	reconnectConfig *ReconnectConfig
	pool            *redis.Pool
	subManager      *SubscriptionManager
	state           int32 // atomic
	reconnectMu     sync.Mutex
}

func NewClient(addr string, password string, db int, cfg *ReconnectConfig) *Client {
	if cfg == nil {
		cfg = DefaultReconnectConfig()
	}
	pool := &redis.Pool{
		MaxIdle:     10,
		MaxActive:   100, // 充足的连接数防止阻塞
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			opts := []redis.DialOption{
				redis.DialConnectTimeout(5 * time.Second),
				redis.DialReadTimeout(5 * time.Second),
				redis.DialWriteTimeout(5 * time.Second),
				redis.DialDatabase(db),
			}
			if password != "" {
				opts = append(opts, redis.DialPassword(password))
			}
			return redis.Dial("tcp", addr, opts...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	c := &Client{
		addr:            addr,
		password:        password,
		database:        db,
		reconnectConfig: cfg,
		pool:            pool,
		state:           int32(StateDisconnected),
	}
	c.subManager = NewSubscriptionManager(pool, c)
	return c
}

// Connect 首次连接
func (c *Client) Connect() error {
	if c.reconnectConfig.OnConnecting != nil {
		c.reconnectConfig.OnConnecting()
	}

	conn := c.pool.Get()
	_, err := conn.Do("PING")
	conn.Close()

	if err != nil {
		// 初始连接失败，立即转入重连流程
		log.Printf("[Redic] 初始连接失败: %v, 启动后台重连...", err)
		atomic.StoreInt32(&c.state, int32(StateReconnecting))
		if c.reconnectConfig.OnDisconnected != nil {
			c.reconnectConfig.OnDisconnected(err)
		}
		go c.reconnectionLoop()
		return err
	}

	atomic.StoreInt32(&c.state, int32(StateConnected))
	if c.reconnectConfig.OnConnected != nil {
		c.reconnectConfig.OnConnected()
	}

	log.Printf("[Redic] 成功连接到 Redis: %s (db=%d)", c.addr, c.database)
	return nil
}

func (c *Client) Close() error {
	// CAS 确保不再触发重连
	oldState := atomic.SwapInt32(&c.state, int32(StateDisconnected))
	if oldState == int32(StateDisconnected) {
		return nil
	}

	c.subManager.Close()
	return c.pool.Close()
}

// --------------------------------------------------------------------------------
// 重连逻辑
// --------------------------------------------------------------------------------

func (c *Client) triggerReconnect(err error) {
	current := atomic.LoadInt32(&c.state)

	// 如果被人工关闭，不重连
	if current == int32(StateDisconnected) {
		return
	}
	// 如果已经在重连中，不重复触发
	if current == int32(StateReconnecting) {
		return
	}

	// 尝试切换到 Reconnecting
	if atomic.CompareAndSwapInt32(&c.state, current, int32(StateReconnecting)) {
		log.Printf("[Redic] 连接异常 (%v)，触发重连流程...", err)
		if c.reconnectConfig.OnDisconnected != nil {
			c.reconnectConfig.OnDisconnected(err)
		}
		go c.reconnectionLoop()
	}
}

func (c *Client) reconnectionLoop() {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()

	cfg := c.reconnectConfig
	attempt := 0
	delay := cfg.InitialDelay

	for {
		// 每次循环前检查是否被外部关闭
		if c.GetState() == StateDisconnected {
			return
		}

		// [修复逻辑判断]
		// 这里必须使用 > (大于)，而不是 >= (大于等于)。
		// 只有当尝试次数严格超过设定值时才放弃。
		// 例如 MaxRetries=3，允许 attempt 0, 1, 2, 3 共4次。
		if cfg.MaxRetries >= 0 && attempt > cfg.MaxRetries {
			atomic.StoreInt32(&c.state, int32(StateFailed))
			log.Printf("[Redic] 达到最大重试次数 (%d)，放弃重连", cfg.MaxRetries)
			if cfg.OnGiveUp != nil {
				cfg.OnGiveUp(errors.New("max retries reached"))
			}
			return
		}

		if cfg.OnReconnecting != nil {
			cfg.OnReconnecting(attempt)
		}

		if c.tryPing() {
			log.Printf("[Redic] 重连成功 (第 %d 次尝试)", attempt+1)
			atomic.StoreInt32(&c.state, int32(StateConnected))

			// 1. 恢复订阅
			go c.subManager.ResubscribeAll()

			// 2. 回调
			if cfg.OnReconnected != nil {
				cfg.OnReconnected(attempt)
			}
			return
		}

		// 失败，增加计数
		attempt++

		// 等待逻辑
		// 只要还没到放弃的条件，就进行等待
		if cfg.MaxRetries < 0 || attempt <= cfg.MaxRetries {
			time.Sleep(delay)
			
			// 计算下次等待时间
			delay = time.Duration(float64(delay) * cfg.BackoffMultiplier)
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
			if cfg.Jitter {
				delay += time.Duration(rand.Int63n(1000)) * time.Millisecond
			}
		}
	}
}

func (c *Client) tryPing() bool {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("PING")
	return err == nil
}

func (c *Client) GetState() ConnectionState {
	return ConnectionState(atomic.LoadInt32(&c.state))
}

// --------------------------------------------------------------------------------
// 对外 API 命令封装
// --------------------------------------------------------------------------------

func (c *Client) Subscribe(channel string, handler func(ch, msg string)) error {
	return c.subManager.Subscribe(channel, handler)
}

func (c *Client) PSubscribe(pattern string, handler func(ch, msg string)) error {
	return c.subManager.PSubscribe(pattern, handler)
}

func (c *Client) Unsubscribe(channels ...string) error {
	return c.subManager.Unsubscribe(channels...)
}

func (c *Client) PUnsubscribe(patterns ...string) error {
	return c.subManager.PUnsubscribe(patterns...)
}

func (c *Client) Publish(channel string, message interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("PUBLISH", channel, message)
	if err != nil && isNetworkError(err) {
		go c.triggerReconnect(err)
	}
	return err
}

func (c *Client) Get(key string) (string, error) {
	conn := c.pool.Get()
	defer conn.Close()
	res, err := redis.String(conn.Do("GET", key))
	if err != nil && isNetworkError(err) {
		go c.triggerReconnect(err)
	}
	return res, err
}

func (c *Client) Set(key string, value interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", key, value)
	if err != nil && isNetworkError(err) {
		go c.triggerReconnect(err)
	}
	return err
}

// isNetworkError 判断是否为网络层错误（需要重连）
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	// 检查预定义错误列表
	for _, ne := range knownNetworkErrors {
		if errors.Is(err, ne) {
			return true
		}
	}
	// 检查系统底层错误
	var errno syscall.Errno
	if errors.As(err, &errno) {
		if _, ok := networkSyscallErrnos[errno]; ok {
			return true
		}
	}
	// 还可以检查是否实现了 Timeout 接口（部分网络错误实现了但不在 error list 中）
	if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
		return true // 超时通常也视为网络不稳定
	}
	return false
}
