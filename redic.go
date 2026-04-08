package redic

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
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

// 哨兵错误
var (
	ErrDisconnected = errors.New("redic: client is disconnected")
	ErrFailed       = errors.New("redic: client has failed, call Connect() to retry")
)

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
	switch s {
	case StateDisconnected:
		return "Disconnected"
	case StateConnecting:
		return "Connecting"
	case StateConnected:
		return "Connected"
	case StateReconnecting:
		return "Reconnecting"
	case StateFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// --------------------------------------------------------------------------------
// 配置结构体
// --------------------------------------------------------------------------------

// PoolConfig 连接池配置
type PoolConfig struct {
	MaxIdle      int
	MaxActive    int
	IdleTimeout  time.Duration
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// DefaultPoolConfig 返回默认连接池配置
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxIdle:      10,
		MaxActive:    100,
		IdleTimeout:  240 * time.Second,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
}

type ReconnectConfig struct {
	MaxRetries        int // -1 表示无限重试
	InitialDelay      time.Duration
	MaxDelay          time.Duration
	BackoffMultiplier float64
	Jitter            bool
	PingTimeout       time.Duration

	// 连接池配置 (nil = 使用默认值)
	Pool *PoolConfig

	// 订阅处理并发度 (0 = CPU核数 * 4)
	SubscriptionWorkerPoolSize int

	// 订阅消息缓冲区大小 (0 = 默认 1024)
	SubscriptionBufferSize int

	// 订阅消息分发超时时间 (0 = 非阻塞丢弃, >0 = 阻塞等待超时后丢弃)
	// 建议设置在 10ms - 500ms 之间，过大可能会导致心跳超时
	SubscriptionDispatchTimeout time.Duration

	// 事件回调
	OnConnecting   func()
	OnConnected    func()
	OnDisconnected func(error)
	OnReconnecting func(attempt int)
	OnReconnected  func(attempt int)
	OnGiveUp       func(error)

	// 消息丢弃回调 (当处理队列满且超时后触发)
	OnMessageDropped func(channel string)
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

// SubscribeOptions 控制单个订阅的消息分发行为
type SubscribeOptions struct {
	// Ordered 为 true 时，handler 在 Receiver 协程中同步执行，
	// 保证同一订阅的消息严格按到达顺序处理。
	// 注意：handler 必须快速返回，否则会阻塞整个订阅连接的消息接收。
	//
	// 为 false（默认）时，handler 通过 Worker Pool 异步执行，
	// 吞吐量更高但不保证顺序。
	Ordered bool
}

type SubscriptionInfo struct {
	Channel   string
	Pattern   string
	Handler   func(channel, message string)
	IsPattern bool
	Ordered   bool // 是否同步顺序分发
}

func (s *SubscriptionInfo) Key() string {
	if s.IsPattern {
		return "pattern:" + s.Pattern
	}
	return "channel:" + s.Channel
}

// --------------------------------------------------------------------------------
// 订阅管理器
// --------------------------------------------------------------------------------

type dispatchTask struct {
	handler func(channel, message string)
	channel string
	message string
}

type SubscriptionManager struct {
	// 保护 subscriptions map
	subMu       sync.RWMutex
	subChannels map[string]*SubscriptionInfo
	subPatterns map[string]*SubscriptionInfo

	// 任务分发通道
	dispatchCh chan *dispatchTask

	// 丢弃消息通知通道 (限制并发回调)
	droppedCh chan string

	// 保护 pubsubConn 对象本身指针的读写
	connMu sync.RWMutex
	// 保护 pubsubConn 的写入操作 (Subscribe/Unsubscribe)，确保线程安全
	writeMu    sync.Mutex
	pubsubConn *redis.PubSubConn

	pool   *redis.Pool
	client *Client
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 状态标志，确保只启动一个接收协程
	receiving int32
	logger    Logger
}

func NewSubscriptionManager(pool *redis.Pool, client *Client, workerSize int) *SubscriptionManager {
	// 获取缓冲区配置
	bufferSize := 1024
	if client.reconnectConfig != nil && client.reconnectConfig.SubscriptionBufferSize > 0 {
		bufferSize = client.reconnectConfig.SubscriptionBufferSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	sm := &SubscriptionManager{
		subChannels: make(map[string]*SubscriptionInfo),
		subPatterns: make(map[string]*SubscriptionInfo),
		dispatchCh:  make(chan *dispatchTask, bufferSize),
		droppedCh:   make(chan string, 100), // 限制并发回调的缓冲区
		pool:        pool,
		ctx:         ctx,
		cancel:      cancel,
		client:      client,
		logger:      client.logger,
	}

	// 启动工作协程池
	if workerSize <= 0 {
		workerSize = runtime.NumCPU() * 4
		if workerSize < 4 {
			workerSize = 4
		}
	}

	for i := 0; i < workerSize; i++ {
		sm.wg.Add(1)
		go sm.workerLoop()
	}

	// 启动丢弃消息处理协程
	sm.wg.Add(1)
	go sm.droppedWorker()

	return sm
}

// ensureConnection 确保连接已建立并启动接收循环
func (sm *SubscriptionManager) ensureConnection() (*redis.PubSubConn, error) {
	sm.connMu.Lock()
	defer sm.connMu.Unlock()

	// 1. 如果已有连接，直接返回
	if sm.pubsubConn != nil {
		return sm.pubsubConn, nil
	}

	// 2. 只有 Client 被显式关闭时才拒绝，其他状态允许尝试从池中获取连接
	if sm.client.GetState() == StateDisconnected {
		return nil, ErrDisconnected
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
	}

	sm.subMu.Lock()
	sm.subChannels[channel] = sub
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
	}

	sm.subMu.Lock()
	sm.subPatterns[pattern] = sub
	sm.subMu.Unlock()

	return sm.doSubscribe(sub)
}

// SubscribeWithOptions 带选项的订阅，支持 Ordered 保序模式
func (sm *SubscriptionManager) SubscribeWithOptions(channel string, handler func(channel, message string), opts SubscribeOptions) error {
	if handler == nil {
		return errors.New("handler cannot be nil")
	}

	sub := &SubscriptionInfo{
		Channel:   channel,
		Handler:   handler,
		IsPattern: false,
		Ordered:   opts.Ordered,
	}

	sm.subMu.Lock()
	sm.subChannels[channel] = sub
	sm.subMu.Unlock()

	return sm.doSubscribe(sub)
}

// PSubscribeWithOptions 带选项的模式订阅
func (sm *SubscriptionManager) PSubscribeWithOptions(pattern string, handler func(channel, message string), opts SubscribeOptions) error {
	if handler == nil {
		return errors.New("handler cannot be nil")
	}

	sub := &SubscriptionInfo{
		Pattern:   pattern,
		Handler:   handler,
		IsPattern: true,
		Ordered:   opts.Ordered,
	}

	sm.subMu.Lock()
	sm.subPatterns[pattern] = sub
	sm.subMu.Unlock()

	return sm.doSubscribe(sub)
}

// 执行底层订阅操作
func (sm *SubscriptionManager) doSubscribe(subscription *SubscriptionInfo) error {
	psc, err := sm.ensureConnection()
	if err != nil {
		sm.logger.Printf("[Redic] 暂时无法发送订阅命令（将在重连时自动恢复）: %v", err)
		return err
	}

	if subscription.IsPattern {
		sm.writeMu.Lock()
		err = psc.PSubscribe(subscription.Pattern)
		sm.writeMu.Unlock()
	} else {
		sm.writeMu.Lock()
		err = psc.Subscribe(subscription.Channel)
		sm.writeMu.Unlock()
	}

	return err
}

func (sm *SubscriptionManager) Unsubscribe(channels ...string) error {
	// 1. 先清理 Map，如果在发送命令前断网，这能保证重连后不会再订阅这些频道
	sm.subMu.Lock()
	for _, ch := range channels {
		delete(sm.subChannels, ch)
	}
	sm.subMu.Unlock()

	// 2. 获取连接操作
	sm.connMu.RLock()
	psc := sm.pubsubConn
	sm.connMu.RUnlock()

	if psc != nil {
		sm.writeMu.Lock()
		defer sm.writeMu.Unlock()
		return psc.Unsubscribe(buildArgs(channels)...)
	}
	return nil
}

func (sm *SubscriptionManager) PUnsubscribe(patterns ...string) error {
	sm.subMu.Lock()
	for _, p := range patterns {
		delete(sm.subPatterns, p)
	}
	sm.subMu.Unlock()

	sm.connMu.RLock()
	psc := sm.pubsubConn
	sm.connMu.RUnlock()

	if psc != nil {
		sm.writeMu.Lock()
		defer sm.writeMu.Unlock()
		return psc.PUnsubscribe(buildArgs(patterns)...)
	}
	return nil
}

func buildArgs(list []string) []interface{} {
	args := make([]interface{}, len(list))
	for i, v := range list {
		args[i] = v
	}
	return args
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
				sm.logger.Printf("[Redic] 订阅连接断开: %v", err)
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

	// 如果正在关闭，不尝试恢复
	if sm.ctx.Err() != nil {
		return
	}

	// 先尝试独立恢复订阅连接，避免将订阅面故障升级为整个客户端的重连事件
	go sm.tryIndependentReconnect()
}

// tryIndependentReconnect 订阅管理器独立恢复连接，不影响命令面
func (sm *SubscriptionManager) tryIndependentReconnect() {
	if sm.GetSubscriptionCount() == 0 {
		return
	}

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-sm.ctx.Done():
				return
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}

		if _, err := sm.ensureConnection(); err == nil {
			sm.logger.Printf("[Redic] 订阅连接独立恢复成功 (第 %d 次尝试)", attempt+1)
			sm.ResubscribeAll()
			return
		}
	}

	// 池也不可用，升级为客户端级重连
	sm.logger.Printf("[Redic] 订阅连接独立恢复失败，升级为客户端级重连")
	if sm.client != nil {
		sm.client.triggerReconnect(errors.New("subscription connection lost, pool unhealthy"))
	}
}

// 重新订阅所有 (重连成功后调用)
func (sm *SubscriptionManager) ResubscribeAll() {
	sm.logger.Printf("[Redic] 开始恢复订阅...")

	// 1. 确保新连接建立
	_, err := sm.ensureConnection()
	if err != nil {
		sm.logger.Printf("[Redic] 恢复订阅失败，无法建立连接: %v", err)
		return
	}

	// 2. 构建批量订阅列表
	sm.subMu.RLock()
	var channels []string
	for ch := range sm.subChannels {
		channels = append(channels, ch)
	}
	var patterns []string
	for p := range sm.subPatterns {
		patterns = append(patterns, p)
	}
	sm.subMu.RUnlock()

	totalCount := len(channels) + len(patterns)
	if totalCount == 0 {
		return
	}

	// 3. 批量发送订阅指令 (优化：优先批量发送，成功则跳过后续单个重试)
	batchSuccess := true
	sm.connMu.RLock()
	psc := sm.pubsubConn
	sm.connMu.RUnlock()

	if psc != nil {
		sm.writeMu.Lock()
		if len(channels) > 0 {
			if err := psc.Subscribe(buildArgs(channels)...); err != nil {
				batchSuccess = false
				sm.logger.Printf("[Redic] 批量订阅 Channels 失败: %v，将降级为逐个重试", err)
			}
		}
		if len(patterns) > 0 {
			if err := psc.PSubscribe(buildArgs(patterns)...); err != nil {
				batchSuccess = false
				sm.logger.Printf("[Redic] 批量订阅 Patterns 失败: %v，将降级为逐个重试", err)
			}
		}
		sm.writeMu.Unlock()
	} else {
		batchSuccess = false
	}

	// 如果批量成功，直接返回，避免冗余的逐个订阅
	if batchSuccess {
		sm.logger.Printf("[Redic] 订阅恢复完成 (批量成功: %d)", totalCount)
		return
	}

	// 4. 降级模式：对所有项进行逐个重试
	count := 0
	sm.subMu.RLock()
	var subs []*SubscriptionInfo
	for _, sub := range sm.subChannels {
		subs = append(subs, sub)
	}
	for _, sub := range sm.subPatterns {
		subs = append(subs, sub)
	}
	sm.subMu.RUnlock()

	for _, sub := range subs {
		exists := false
		sm.subMu.RLock()
		if sub.IsPattern {
			_, exists = sm.subPatterns[sub.Pattern]
		} else {
			_, exists = sm.subChannels[sub.Channel]
		}
		sm.subMu.RUnlock()
		if !exists {
			continue
		}
		ok := false
		lastErr := error(nil)
		delay := 100 * time.Millisecond
		for attempt := 0; attempt < 3; attempt++ {
			if err := sm.doSubscribe(sub); err != nil {
				lastErr = err
				time.Sleep(delay)
				delay *= 2
				if delay > time.Second {
					delay = time.Second
				}
				continue
			}
			ok = true
			break
		}
		if ok {
			count++
		} else if lastErr != nil {
			sm.logger.Printf("[Redic] 恢复订阅 %s 失败: %v", sub.Key(), lastErr)
		}
	}
	sm.logger.Printf("[Redic] 订阅恢复完成 (逐个重试: 成功 %d / 总数 %d)", count, totalCount)
}

func (sm *SubscriptionManager) handleMessage(msg interface{}) {
	// metrics: count message processed and start time
	if sm.client != nil {
		if atomic.AddInt64(&sm.client.messagesProcessed, 1) == 1 {
			// first message observed
			sm.client.metricsMu.Lock()
			if sm.client.messagesStartAt.IsZero() {
				sm.client.messagesStartAt = time.Now()
			}
			sm.client.metricsMu.Unlock()
		}
	}
	switch v := msg.(type) {
	case redis.Message:
		// Redigo 中 redis.Message 结构体同时处理普通消息和模式消息
		// 如果 Pattern 字段不为空，则是 PMessage
		if v.Pattern != "" {
			sm.dispatchPatternMessage(v)
		} else {
			sm.dispatch(v.Channel, string(v.Data))
		}
	case redis.Subscription:
		// 订阅/退订成功通知，一般忽略
	case redis.Pong:
		// 心跳响应，忽略
	}
}

func (sm *SubscriptionManager) dispatch(channel string, data string) {
	sm.subMu.RLock()
	sub, ok := sm.subChannels[channel]
	sm.subMu.RUnlock()

	if !ok || sub.Handler == nil {
		return
	}

	// Ordered 模式：同步调用，保证严格顺序
	if sub.Ordered {
		sm.invokeHandler(sub.Handler, channel, data)
		return
	}

	sm.submitTask(&dispatchTask{
		handler: sub.Handler,
		channel: channel,
		message: data,
	})
}

func (sm *SubscriptionManager) dispatchPatternMessage(msg redis.Message) {
	sm.subMu.RLock()
	sub, ok := sm.subPatterns[msg.Pattern]
	sm.subMu.RUnlock()

	if !ok || sub.Handler == nil {
		return
	}

	if sub.Ordered {
		sm.invokeHandler(sub.Handler, msg.Channel, string(msg.Data))
		return
	}

	sm.submitTask(&dispatchTask{
		handler: sub.Handler,
		channel: msg.Channel,
		message: string(msg.Data),
	})
}

// invokeHandler 同步调用 handler（用于 Ordered 模式），带 panic 保护
func (sm *SubscriptionManager) invokeHandler(handler func(string, string), channel, data string) {
	defer func() {
		if r := recover(); r != nil {
			sm.logger.Printf("[Redic] Handler panic: %v", r)
		}
	}()
	handler(channel, data)
}

// 统一的提交任务逻辑，支持超时和回调
func (sm *SubscriptionManager) submitTask(task *dispatchTask) {
	// 快速路径：非阻塞尝试
	select {
	case sm.dispatchCh <- task:
		return
	default:
		// 队列已满
	}

	// 慢路径：检查是否配置了超时
	var timeout time.Duration
	if sm.client.reconnectConfig != nil {
		timeout = sm.client.reconnectConfig.SubscriptionDispatchTimeout
	}

	if timeout > 0 {
		timer := time.NewTimer(timeout)
		select {
		case sm.dispatchCh <- task:
			timer.Stop()
			return
		case <-timer.C:
			// 超时仍未写入，执行丢弃逻辑
		}
	}

	// 最终丢弃逻辑
	sm.logger.Printf("[Redic] 警告: 订阅消息处理队列已满，丢弃消息 (Channel: %s)", task.channel)
	if sm.client.reconnectConfig != nil && sm.client.reconnectConfig.OnMessageDropped != nil {
		// 使用非阻塞发送，如果 droppedCh 也满了，则不再触发回调，防止无限创建 Goroutine
		select {
		case sm.droppedCh <- task.channel:
		default:
			// 丢弃通知通道已满，跳过回调
		}
	}
}

func (sm *SubscriptionManager) workerLoop() {
	defer sm.wg.Done()
	for {
		select {
		case <-sm.ctx.Done():
			return
		case task := <-sm.dispatchCh:
			func() {
				defer func() {
					if r := recover(); r != nil {
						sm.logger.Printf("[Redic] Handler panic: %v", r)
					}
				}()
				task.handler(task.channel, task.message)
			}()
		}
	}
}

func (sm *SubscriptionManager) droppedWorker() {
	defer sm.wg.Done()
	for {
		select {
		case <-sm.ctx.Done():
			return
		case ch := <-sm.droppedCh:
			if sm.client.reconnectConfig != nil && sm.client.reconnectConfig.OnMessageDropped != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							sm.logger.Printf("[Redic] OnMessageDropped panic: %v", r)
						}
					}()
					sm.client.reconnectConfig.OnMessageDropped(ch)
				}()
			}
		}
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
	sm.subChannels = make(map[string]*SubscriptionInfo)
	sm.subPatterns = make(map[string]*SubscriptionInfo)
	sm.subMu.Unlock()

	sm.wg.Wait() // 等待接收协程彻底退出
}

func (sm *SubscriptionManager) GetSubscriptionCount() int {
	sm.subMu.RLock()
	defer sm.subMu.RUnlock()
	return len(sm.subChannels) + len(sm.subPatterns)
}

// --------------------------------------------------------------------------------
// Client (主客户端)
// --------------------------------------------------------------------------------

type Client struct {
	// 64-bit fields first for atomic alignment on 32-bit systems
	reconnectAttempts int64
	messagesProcessed int64

	addr                string
	password            string
	database            int
	reconnectConfig     *ReconnectConfig
	pool                *redis.Pool
	subManager          *SubscriptionManager
	state               int32 // atomic
	reconnectMu         sync.Mutex
	cmdTimeoutCount     int32
	cmdTimeoutThreshold int
	logger              Logger

	metricsMu       sync.Mutex // protects messagesStartAt
	messagesStartAt time.Time

	// notification channel for fast shutdown
	doneCh chan struct{}
}

func NewClient(addr string, password string, db int, cfg *ReconnectConfig) *Client {
	if cfg == nil {
		cfg = DefaultReconnectConfig()
	}
	// Make a defensive copy of config
	cfgCopy := *cfg

	// 解析连接池配置
	pc := cfg.Pool
	if pc == nil {
		pc = DefaultPoolConfig()
	}

	pool := &redis.Pool{
		MaxIdle:     pc.MaxIdle,
		MaxActive:   pc.MaxActive,
		Wait:        true,
		IdleTimeout: pc.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			opts := []redis.DialOption{
				redis.DialConnectTimeout(pc.DialTimeout),
				redis.DialReadTimeout(pc.ReadTimeout),
				redis.DialWriteTimeout(pc.WriteTimeout),
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
		reconnectConfig: &cfgCopy,
		pool:            pool,
		state:           int32(StateDisconnected),
		doneCh:          make(chan struct{}),
	}
	rand.Seed(time.Now().UnixNano())
	c.cmdTimeoutThreshold = 3
	c.logger = log.Default()

	workerSize := 0
	if cfg != nil {
		workerSize = cfg.SubscriptionWorkerPoolSize
	}
	c.subManager = NewSubscriptionManager(pool, c, workerSize)
	return c
}

type Logger interface {
	Printf(string, ...interface{})
}

func (c *Client) SetLogger(l Logger) {
	c.logger = l
	if c.subManager != nil {
		c.subManager.logger = l
	}
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
		c.logger.Printf("[Redic] 初始连接失败: %v, 启动后台重连...", err)
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

	c.logger.Printf("[Redic] 成功连接到 Redis: %s (db=%d)", c.addr, c.database)
	return nil
}

func (c *Client) Close() error {
	// CAS 确保不再触发重连
	oldState := atomic.SwapInt32(&c.state, int32(StateDisconnected))
	if oldState == int32(StateDisconnected) {
		return nil
	}

	// 通知后台重连循环立即退出
	close(c.doneCh)

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
	// StateFailed 是终态，不自动重连，需要显式调用 Reconnect()
	if current == int32(StateFailed) {
		return
	}

	// 尝试切换到 Reconnecting
	if atomic.CompareAndSwapInt32(&c.state, current, int32(StateReconnecting)) {
		c.logger.Printf("[Redic] 连接异常 (%v)，触发重连流程...", err)
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

		// metrics: record a reconnect attempt (counts success and failure)
		atomic.AddInt64(&c.reconnectAttempts, 1)

		// [修复逻辑判断]
		// 这里必须使用 >= (大于等于)。
		// 只有当尝试次数达到设定值时才放弃。
		// 例如 MaxRetries=0，允许 attempt 0 (1次)。0 >= 0 True -> 退出。
		if cfg.MaxRetries >= 0 && attempt >= cfg.MaxRetries {
			atomic.StoreInt32(&c.state, int32(StateFailed))
			c.logger.Printf("[Redic] 达到最大重试次数 (%d)，放弃重连", cfg.MaxRetries)
			if cfg.OnGiveUp != nil {
				cfg.OnGiveUp(errors.New("max retries reached"))
			}
			return
		}

		if cfg.OnReconnecting != nil {
			cfg.OnReconnecting(attempt)
		}

		if c.tryPing() {
			c.logger.Printf("[Redic] 重连成功 (第 %d 次尝试)", attempt+1)
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
			select {
			case <-c.doneCh:
				return
			case <-time.After(delay):
			}

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

	// 使用配置的 PingTimeout
	timeout := c.reconnectConfig.PingTimeout
	if timeout <= 0 {
		timeout = defaultPingTimeout
	}
	if dc, ok := conn.(interface{ SetDeadline(time.Time) error }); ok {
		dc.SetDeadline(time.Now().Add(timeout))
	}

	_, err := conn.Do("PING")
	return err == nil
}

func (c *Client) GetState() ConnectionState {
	return ConnectionState(atomic.LoadInt32(&c.state))
}

// Reconnect 从 Failed 状态显式重启重连流程
func (c *Client) Reconnect() error {
	current := c.GetState()
	if current == StateDisconnected {
		return ErrDisconnected
	}
	if current == StateConnected || current == StateReconnecting {
		return nil // 已经连接或正在重连
	}
	// 从 Failed 或其他状态切入重连
	if atomic.CompareAndSwapInt32(&c.state, int32(current), int32(StateReconnecting)) {
		// 重置 doneCh，以便新的重连循环可以正常等待
		c.reconnectMu.Lock()
		select {
		case <-c.doneCh:
			// doneCh 已关闭，创建新的
			c.doneCh = make(chan struct{})
		default:
		}
		c.reconnectMu.Unlock()

		c.logger.Printf("[Redic] 手动触发重连流程...")
		go c.reconnectionLoop()
	}
	return nil
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

// SubscribeWithOptions 带选项的订阅（支持 Ordered 保序模式）
func (c *Client) SubscribeWithOptions(channel string, handler func(ch, msg string), opts SubscribeOptions) error {
	return c.subManager.SubscribeWithOptions(channel, handler, opts)
}

// PSubscribeWithOptions 带选项的模式订阅
func (c *Client) PSubscribeWithOptions(pattern string, handler func(ch, msg string), opts SubscribeOptions) error {
	return c.subManager.PSubscribeWithOptions(pattern, handler, opts)
}

// checkReady 检查客户端是否处于可用状态
func (c *Client) checkReady() error {
	switch c.GetState() {
	case StateDisconnected:
		return ErrDisconnected
	case StateFailed:
		return ErrFailed
	default:
		return nil
	}
}

func (c *Client) Publish(channel string, message interface{}) error {
	if err := c.checkReady(); err != nil {
		return err
	}
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("PUBLISH", channel, message)
	if err != nil {
		c.maybeTriggerReconnectOnError(err)
	}
	return err
}

func (c *Client) Get(key string) (string, error) {
	if err := c.checkReady(); err != nil {
		return "", err
	}
	conn := c.pool.Get()
	defer conn.Close()
	res, err := redis.String(conn.Do("GET", key))
	if err != nil {
		c.maybeTriggerReconnectOnError(err)
	}
	return res, err
}

func (c *Client) Set(key string, value interface{}) error {
	if err := c.checkReady(); err != nil {
		return err
	}
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", key, value)
	if err != nil {
		c.maybeTriggerReconnectOnError(err)
	}
	return err
}

// Do 执行任意 Redis 命令，与 Redigo 接口兼容
func (c *Client) Do(commandName string, args ...interface{}) (interface{}, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	conn := c.pool.Get()
	defer conn.Close()
	reply, err := conn.Do(commandName, args...)
	if err != nil {
		c.maybeTriggerReconnectOnError(err)
	}
	return reply, err
}

// DoContext 支持 context 的命令执行
func (c *Client) DoContext(ctx context.Context, commandName string, args ...interface{}) (interface{}, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	conn := c.pool.Get()
	defer conn.Close()
	if dc, ok := conn.(redis.ConnWithContext); ok {
		reply, err := dc.DoContext(ctx, commandName, args...)
		if err != nil {
			c.maybeTriggerReconnectOnError(err)
		}
		return reply, err
	}
	// fallback: 不支持 context 的连接
	reply, err := conn.Do(commandName, args...)
	if err != nil {
		c.maybeTriggerReconnectOnError(err)
	}
	return reply, err
}

// isNetworkError 判断是否为网络层错误（需要重连）
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	for _, nne := range nonNetworkErrors {
		if errors.Is(err, nne) {
			return false
		}
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

func (c *Client) maybeTriggerReconnectOnError(err error) {
	if err == nil {
		return
	}
	if te, ok := err.(interface{ Timeout() bool }); ok && te.Timeout() {
		if int(atomic.AddInt32(&c.cmdTimeoutCount, 1)) >= c.cmdTimeoutThreshold {
			atomic.StoreInt32(&c.cmdTimeoutCount, 0)
			go c.triggerReconnect(err)
		}
		return
	}
	if isNetworkError(err) {
		go c.triggerReconnect(err)
	}
}

// Metrics 暴露指标
type Metrics struct {
	ReconnectAttempts int64
	State             ConnectionState
	SubscriptionCount int
	MessagesProcessed int64
	MessagesPerSecond float64
}

func (c *Client) GetMetrics() Metrics {
	var rate float64
	mp := atomic.LoadInt64(&c.messagesProcessed)

	c.metricsMu.Lock()
	start := c.messagesStartAt
	c.metricsMu.Unlock()

	if mp > 0 && !start.IsZero() {
		sec := time.Since(start).Seconds()
		if sec > 0 {
			rate = float64(mp) / sec
		}
	}
	subCount := 0
	if c.subManager != nil {
		subCount = c.subManager.GetSubscriptionCount()
	}
	return Metrics{
		ReconnectAttempts: atomic.LoadInt64(&c.reconnectAttempts),
		State:             c.GetState(),
		SubscriptionCount: subCount,
		MessagesProcessed: mp,
		MessagesPerSecond: rate,
	}
}
