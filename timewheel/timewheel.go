package timewheel

import (
	"sync"
	"time"
)

func test()  {
	wheel := NewTimeWheel(10 * time.Millisecond, 10)
	wheel.Add(Every(10 * time.Second))
}

// 创建一个新的时间轮
func NewTimeWheel(tickNs time.Duration, size int) *TimeWheel {
	return &TimeWheel{
		tickNs:    int64(tickNs),
		wheelSize: size,
	}
}

// 超时回调
type ExpirationCallback func(time *time.Time)

// 计算超时时间的回调
type ExpirationTimeCallback func(time *time.Time) int64
//type TickHandler func(time *time.Time, spec *timerSpec)

// 每隔一定间隔定时触发
func Every(duration time.Duration) ExpirationTimeCallback {
	return func(now *time.Time) int64 {
		return now.UnixNano() + int64(duration)
	}
}

// 触发一次
func Once(duration time.Duration) ExpirationTimeCallback {
	isFirst := false
	return func(time *time.Time) int64 {
		if isFirst {
			return 0
		} else {
			isFirst = true
			return time.UnixNano() + int64(duration)
		}
	}
}

// 控制命令
type cmd struct {
	code cmdCode
	data interface{}
}

type cmdCode byte
const (
	stop cmdCode = 1 + iota
	add
	remove
)

// 计时任务句柄
type TaskHandler struct {
	id int
	timingWheel *TimeWheel
	expirationChan chan *time.Time
}


// 时间轮
type TimeWheel struct {
	nextId 		 int
	idLock sync.Mutex
	tickNs       int64          // tick间隔
	wheelSize    int            // 时间轮大小
	startNs      int64          // 开始时间
	currentNs    int64          // 当前UNIX 纳秒数
	tickingWheel *overflowWheel //  时间轮
	controlChan  chan *cmd       // 控制信号频道
	ticker       *time.Ticker   // 定时器
}

// 启动时间轮
// TODO 非线程安全
func (wheel *TimeWheel) Start()  {
	if wheel.tickNs <= 0 || wheel.wheelSize <= 0 {
		return
	}
	if wheel.ticker != nil {
		return
	}

	// startNs & currentNs
	now := time.Now()
	t := now.UnixNano()
	wheel.startNs = t - (t % wheel.tickNs) // 向下取整
	wheel.currentNs = wheel.startNs

	wheel.tickingWheel = &overflowWheel{
		startNs:   wheel.startNs,
		tickNs:    wheel.tickNs,
		currentNs: wheel.currentNs,
		interval:  wheel.tickNs * int64(wheel.wheelSize),
		buckets:   make([]*timerTaskEntry, 0, wheel.wheelSize),
		pre:       nil,
		next:      nil,
	}

	wheel.controlChan = make(chan *cmd)
	wheel.ticker = time.NewTicker(time.Duration(wheel.tickNs))

	// 开启时间循环
	wheel.loopAsync()
}

// 添加一个定时任务, 添加前请确保该时间轮已经调用了Start,否则会报错
func (wheel *TimeWheel) Add(timeCb ExpirationTimeCallback) *TaskHandler {
	if wheel.ticker == nil || wheel.tickingWheel == nil || wheel.controlChan == nil {
		panic("make sure having started before adding")
	}
	now := time.Now()
	c := make(chan *time.Time)
	entry := &timerTaskEntry{
		id: wheel.genNextId(),
		expirationChan: c,
		pre:         nil,
		next:        nil,
		spec:        &timerSpec{
			expirationTimeCb: timeCb,
			callback: nil,
		},
		triggerTime: timeCb(&now),
	}

	// 发送添加定时任务的命令
	wheel.sendCmd(add, entry)

	return &TaskHandler{
		id:             entry.id,
		timingWheel:    wheel,
		expirationChan: c,
	}
}

// 组装并发送命令
func (wheel *TimeWheel) sendCmd(code cmdCode, data interface{})  {
	wheel.controlChan <- &cmd{
		code: code,
		data: data,
	}
}

// 开启一个异步循环tick
func (wheel *TimeWheel) loopAsync()  {
	// 所有的操作都在这个程中进行
	go func() {
		for true {
			select {
			case t := <- wheel.ticker.C:
				// 处理超时
				wheel.onTick(&t)
			case cmd := <-wheel.controlChan:
				// 处理控制命令
				wheel.onCmd(cmd)
			}
		}
	}()
}

// 计时器超时
func (wheel *TimeWheel) onTick(time *time.Time)  {
	// 转动时间轮
	wheel.tickingWheel.tick(time)
}

// 处理各种控制命令的函数
func (wheel *TimeWheel) onCmd(cmd *cmd)  {
	switch cmd.code {
	case stop:
		// TODO
	case add:
		entry := cmd.data.(*timerTaskEntry)
		// 因为添加任务时需要返回任务ID,所以已经在添加时生成了ID
		//entry.id = wheel.nextId
		//wheel.nextId += 1
		wheel.tickingWheel.addTaskEntry(entry)
	}
}

// 生成并更新计时任务的下一个ID,此函数线程安全
func (wheel *TimeWheel) genNextId() int {
	wheel.idLock.Lock()
	defer wheel.idLock.Unlock()

	wheel.nextId += 1
	return wheel.nextId
}

// 定时器规范,描述如何触发
type timerSpec struct {
	expirationTimeCb ExpirationTimeCallback
	callback ExpirationCallback
}

// (超时)回调节点
type timerTaskEntry struct {
	id int // 任务ID
	pre *timerTaskEntry  // 前继节点
	next *timerTaskEntry // 后续节点
	spec *timerSpec      // 回调规范
	triggerTime int64    // 触发时间戳, 该时间每次触发后更新
	expirationChan chan *time.Time // 超时频道
}

// 转轮
type overflowWheel struct {
	startNs   int64 // 时间轮开始时间
	tickNs    int64 // tick间隔
	currentNs int64 // 当前时间
	interval  int64 // 转一圈的时间
	buckets   []*timerTaskEntry
	pre       *overflowWheel
	next      *overflowWheel
}

// 时间轮第一次tick需要执行的操作
func (w *overflowWheel) tick(time *time.Time)  {
	// 如果还不到tick时间,直接返回
	if w.currentNs + w.tickNs > time.UnixNano() {
		return
	}
	w.updateCurrentNs(time)

	// 执行回调,只有最里面的轮子需要执行
	if w.pre != nil {
		// 如果是次级轮,则将当前游标处节点降级
		bucketId := w.calculateCurrentBucket()
		if node := w.buckets[bucketId]; node != nil {
			w.buckets[bucketId] = nil
			w.pre.downGrade(node)
		}
		return
	}

	// 最里层的时间轮,直接执行
	w.execute(time)
}

// 将超时任务添加到时间轮
func (w *overflowWheel) addTaskEntry(entry *timerTaskEntry) bool {
	// 如果触发时间超过了当前时间轮的范围,就要安装给下一级的时间轮,否则直接安装在本时间轮
	if w.currentNs + w.interval >= entry.triggerTime {
		bucketId := w.calculateCurrentBucket()
		if node := w.buckets[bucketId]; node == nil {
			w.buckets[bucketId] = entry
			entry.pre = nil
		} else {
			// TODO 有BUG,应该是连接到最后一个,现在是直接连接到了第一个的后面
			node.next = entry
			entry.pre = node
		}
		return true
	} else if w.currentNs + w.tickNs > entry.triggerTime {
		// 已经过期了
		// 看看上一级更精确的时间轮是否可以
		if w.pre == nil {
			return false
		} else {
			return w.downGrade(entry)
		}
	} else {
		// 过载了
		return w.overflow(entry)
	}
}

func (w *overflowWheel) overflow(entry *timerTaskEntry) bool {
	// 创建新时间轮
	w.addOverflowWheel()
	// 将超时任务添加到下一级的时间轮
	return w.next.addTaskEntry(entry)
}

// 如果超载了,创建下一级的超载时间轮
func (w *overflowWheel) addOverflowWheel()  {
	if w.next == nil {
		w.next = &overflowWheel{}

		// 初始化各值
		temp := w.next
		temp.startNs = w.currentNs
		temp.currentNs = w.currentNs
		temp.tickNs = w.interval
		bucketSize := temp.tickNs * int64(len(w.buckets))
		temp.interval = bucketSize
		temp.buckets = make([]*timerTaskEntry, bucketSize)
		temp.pre = w
		temp.next = nil
	}
}

// 执行超时回调
func (w *overflowWheel) execute(time *time.Time)  {
	// TODO 实现
	// TODO 判断是否需要继续定时
	currentBucketId := w.calculateCurrentBucket()
	if node := w.buckets[currentBucketId]; node != nil {
		w.executeLink(node, time)
	}
}

// 链式执行所有任务
func (w *overflowWheel) executeLink(node *timerTaskEntry, time *time.Time)  {
	// 有回调就调用回调,否则往超时信道里发送超时消息
	if node.spec.callback == nil {
		node.spec.callback(time)
	} else {
		node.expirationChan <- time
	}

	if node.next != nil {
		w.executeLink(node.next, time)
	}
}

func (w *overflowWheel) ifContinue(node *timerTaskEntry) bool {
	// TODO 完成真实逻辑
	return false
}

// 将任务链降级到上一个更精确的时间轮, 本函数不再检查对应的时间轮是否存在
func (w *overflowWheel) downGrade(node *timerTaskEntry) bool {
	return w.pre.addTaskEntry(node)
}

// 更新当前时间
func (w *overflowWheel) updateCurrentNs(time *time.Time)  {
	t := time.UnixNano()
	w.currentNs = t - (t % w.tickNs)
}

// 计算当前时间对应的bucket
func (w *overflowWheel) calculateCurrentBucket() int {
	return int(w.currentNs / w.tickNs % int64(len(w.buckets)))
}

// 默认的超时回调
func defaultTickHandler(time *time.Time, callback ExpirationCallback)  {

}

