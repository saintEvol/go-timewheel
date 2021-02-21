package timewheel

import (
	"sync"
	"time"
)

// 创建一个新的时间轮
func NewTimeWheel(tickNs time.Duration, size int) *TimeWheel {
	return &TimeWheel{
		tickNs:    int64(tickNs),
		wheelSize: size,
	}
}

// 超时回调
type ExpirationCallback func(time *time.Time)

// 计算超时时间的回调, 每次超时后,时间轮会调用此回调计算出下次超时时间,如果超时时间大于当前值, 则任务继续添加,否则,移除任务
type ExpirationTimeCallback func(time *time.Time) int64
//type TickHandler func(time *time.Time, spec *timerSpec)

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
	isComplete bool
	timingWheel *TimeWheel
	expirationChan chan *time.Time
}

// 移除本任务
func (h *TaskHandler) Remove()  {
	h.timingWheel.sendCmd(remove, h.id)
	h.expirationChan = nil
}

func (h *TaskHandler) Wait() (now *time.Time, ok bool)  {
	now, ok = <- h.expirationChan
	if !ok {
		h.expirationChan = nil
	}
	return
}

// 将状态标记为已经完成,标记后,外部调用者还能获取到一次chan,之后,chan被置为nil
func (h *TaskHandler) complete()  {
	h.isComplete = true
}

// 定时任务完成时由时间轮调用
func (h *TaskHandler) onComplete()  {
	//fmt.Printf("---- >>> ------ set complete: %d \r\n", h.id)
	h.complete()
}

/*
注意: 此接口只保证在任务完成后,还可获得一次chan,之后返回nil,以方便配合select使用
注意: 如果直接select 此函数, 虽然任务完成后,因为会将通道设置为nil,所以Select会阻塞此通信,
	 但是,select仍然会不停通过此函数获取通道,这里可能会造成性能损耗
*/
// TODO 优化chan()性能
func (h *TaskHandler) Chan() <- chan *time.Time {
	ret := h.expirationChan
	//fmt.Printf("[[][][][][][] before check: id: %d \r\n", h.id)
	if h.isComplete {
		// TODO TEST Chan
		// fmt.Printf("[[][][][][][] before complete: id: %d \r\n", h.id)
		//_, ok := <- ret
		// TODO 期望是ok = false
		//fmt.Printf("[][][][][][[] complete: id: %d, ok: -> %v \r\n", h.id, ok)
		h.expirationChan = nil
	}
	// fmt.Printf("return chan of %d, is null: %v\r\n", h.id, ret == nil)
	return ret
}

func (h *TaskHandler) Id() int {
	return h.id
}


// 时间轮
type TimeWheel struct {
	nextId 		 int
	bufferSize int
	idLock sync.Mutex
	tickNs       int64          // tick间隔
	wheelSize    int            // 时间轮大小
	startNs      int64          // 开始时间
	currentNs    int64          // 当前UNIX 纳秒数
	tickingWheel *overflowWheel //  时间轮
	controlChan  chan *cmd       // 控制信号频道
	ticker       *time.Ticker   // 定时器
	tasks map[int]*timerTaskEntry // 该定时器所有的任务
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
	wheel.startNs = uniformNs(t, wheel.tickNs) // 向下归整时间
	wheel.currentNs = wheel.startNs

	wheel.tickingWheel = &overflowWheel{
		startNs:   wheel.startNs,
		tickNs:    wheel.tickNs,
		currentNs: wheel.currentNs,
		interval:  wheel.tickNs * int64(wheel.wheelSize),
		pre:       nil,
		next:      nil,
	}
	wheel.tickingWheel.initBuckets(wheel.wheelSize)

	// TODO 控制chan的长度是否需要优化
	wheel.controlChan = make(chan *cmd, wheel.bufferSize)
	wheel.ticker = time.NewTicker(time.Duration(wheel.tickNs))
	wheel.tasks = make(map[int]*timerTaskEntry)

	// 开启时间循环
	wheel.loopAsync()
}

// 停止定时器
// todo 非线程安全
func (wheel *TimeWheel) Stop()  {
	wheel.sendCmd(stop, nil)
}

// 添加一个定时任务, 添加前请确保该时间轮已经调用了Start,否则会报错
func (wheel *TimeWheel) Add(timeCb ExpirationTimeCallback) *TaskHandler {
	if wheel.ticker == nil || wheel.tickingWheel == nil || wheel.controlChan == nil {
		panic("make sure having started before adding")
	}
	now := time.Now()
	// TODO 超时chan的长度是否需要优化

	c := make(chan *time.Time)
	entryId := wheel.genNextId()
	handler := &TaskHandler{
		id: entryId,
		timingWheel: wheel,
		expirationChan: c,
	}

	// TODO 可以考虑在时间轮协程里初始化定时任务的数据
	entry := &timerTaskEntry {
		id: entryId,
		onCompleteHandler: handler.onComplete,
		expirationChan: c,
		pre:         nil,
		next:        nil,
		spec:        &timerSpec{
			expirationTimeCb: timeCb,
			callback: nil,
		},
		triggerTime: uniformNs(timeCb(&now), wheel.tickNs),
	}

	// 发送添加定时任务的命令
	wheel.sendCmd(add, entry)

	return handler
}

// 睡眠一段时间
func (wheel * TimeWheel) Sleep(duration time.Duration)  {
	wheel.Add(Once(duration)).Wait()
}

func (wheel *TimeWheel) TaskCount() int {
	return len(wheel.tasks)
}

// 设置缓冲大小, 只有在调用Start之前设置才会生效
func (wheel *TimeWheel) WithBufferSize(size int)  {
	wheel.bufferSize = size
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

//var tickCnt = 0
// 计时器超时
func (wheel *TimeWheel) onTick(now *time.Time)  {
	// 转动时间轮
	//old := wheel.currentNs
	wheel.currentNs = uniformNs(now.UnixNano(), wheel.tickNs)
	// 可能由于各种原因,实际tick间隔大于规定的间隔,需要进行补偿
	//fmt.Printf("--------- start real tick, cnt: %d, ms: %d, diff: %d now:%d ---------\r\n",
	//	tickCnt + 1, int64(tickCnt + 1) * wheel.tickNs / int64(time.Millisecond), wheel.currentNs - old, now.UnixNano())
	// 不再补偿
	wheel.tickingWheel.tick(wheel, now)
	//for i := old + wheel.tickNs; i <= wheel.currentNs; i += wheel.tickNs {
	//	tickCnt += 1
	//	// TODO TEST ONTICK
	//	//fmt.Printf("--------- start tick, cnt: %d, ms: %d ---------\r\n",
	//	//	tickCnt, int64(tickCnt) * wheel.tickNs / int64(time.Millisecond))
	//	temp := time.Unix(0, i)
	//	wheel.tickingWheel.tick(wheel, &temp)
	//}
	//fmt.Printf("--------- end tick ---------\r\n")
}

// 处理各种控制命令的函数
func (wheel *TimeWheel) onCmd(cmd *cmd)  {
	switch cmd.code {
	case stop:
		wheel.ticker.Stop()
		// 不将ticker置空,防止有地方还在访问
		//wheel.ticker = nil
		close(wheel.controlChan)
		wheel.controlChan = nil
		// 释放所有时间轮
		if wheel.tickingWheel != nil {
			wheel.tickingWheel.dispose()
		}
		wheel.tasks = nil
	case add:
		entry := cmd.data.(*timerTaskEntry)
		// 因为添加任务时需要返回任务ID,所以已经在添加时生成了ID
		//entry.id = wheel.nextId
		//wheel.nextId += 1
		// 先将任务放入全局map
		wheel.tasks[entry.id] = entry
		// 装载进时间轮
		wheel.tickingWheel.addTaskEntry(entry)

		//TODO TEST -- ADD TASK OVER
		//fmt.Printf(">>> add task over\r\n")
	case remove:
		id := cmd.data.(int)
		if data, ok := wheel.tasks[id]; ok {
			// 删除节点
			wheel.deleteTaskMap(id)
			data.cancel()
		}
	}
}

// 生成并更新计时任务的下一个ID,此函数线程安全
// TODO 考虑优化成不加锁,而通过chan将ID返回给调用者
func (wheel *TimeWheel) genNextId() int {
	wheel.idLock.Lock()
	defer wheel.idLock.Unlock()

	wheel.nextId += 1
	return wheel.nextId
}

//
func (wheel *TimeWheel) deleteTaskMap(id int)  {
	delete(wheel.tasks, id)
}

// 定时器规范,描述如何触发
type timerSpec struct {
	expirationTimeCb ExpirationTimeCallback
	callback ExpirationCallback
}

// 生成一个空节点
func emptyTaskEntry() *timerTaskEntry {
	return &timerTaskEntry{
		id:             0,
		pre:            nil,
		next:           nil,
		spec:           nil,
		triggerTime:    0,
		expirationChan: nil,
	}
}

// (超时)回调节点
type timerTaskEntry struct {
	id int // 任务ID
	// canceled bool // 是否已经取消
	// 前继节点, 第一个节点的pre指向最后一个节点
	// 添加新节点时,总是将新节点添加到第一个位置,这样,链表实际上是一个反向链表
	// 为了保证执行顺序,因此执行时,会根据第一个节点的pre取到最后一个节点,再从后往前倒序执行
	// 因为是添加到头部,所以节点的添加是O(1)
	// 因为是一个链表,所以根据节点指针删除节点也是O(10
	// 每个节点都通过id:node映射到全局map中
	pre *timerTaskEntry
	next *timerTaskEntry // 后续节点
	spec *timerSpec      // 回调规范
	triggerTime int64    // 触发时间戳, 该时间每次触发后更新, 此时间是根据tickNs下向取整后的时间
	onCompleteHandler func() // 计时任务完成时的回调
	expirationChan chan *time.Time // 超时频道
}

// 将本节点所在的任务链和tail节点所在的任务链连接起来, 以本节点所在的任务链为头部
// 两个操作节点最好是各自任务链的头节点, 否则效率低下
func (e *timerTaskEntry) concat(tail *timerTaskEntry)  {
	hh := e.findHeadNode() // 头部的头节点
	th := tail.findHeadNode() // 尾部的头节点
	if hh == th {
		// 如果二者的着呢部节点一样,说明已经是同一个任务链了
		return
	}
	ht := hh.pre // 头部的尾节点
	tt := th.pre // 尾部的尾节点

	/**
		1. 头部的头节点的pre 指向尾部的尾节点
		2. 头部的尾节点的next 指向尾部的头节点
		3. 尾部的头节点的pre指向头部的尾节点
	 */
	hh.pre = tt
	if ht != th {
		ht.next = th
	}
	th.pre = ht
}

// 查找头节点,如果本节点不是任务链中的一部分,则将本节点变成一个任务链
func (e *timerTaskEntry) findHeadNode() *timerTaskEntry {
	if e.pre == nil {
		// 不是任务链的一部分
		e.pre = e
		return e
	}

	if e.isHead() {
		return e
	}
	return e.pre.findHeadNode()
}

func (e *timerTaskEntry) findTailNode() *timerTaskEntry {
	h := e.findHeadNode()
	return h.pre
}

func (e *timerTaskEntry) cancel() {
	if e.expirationChan != nil {
		close(e.expirationChan)
		e.expirationChan = nil
	}
	e.detach()
}

// 将节点从任务链中卸除
func (e *timerTaskEntry) detach() {
	e.exchange()
	// 将前置和后续置空
	e.pre = nil
	e.next = nil
	// 每个槽位都有一个默认的空节点,所以不可能是头节点
	//if e.isHead() {
	//	if e.next != nil {
	//		effectId = e.next.id
	//		ifEffect = true
	//	}
	//	e.clone(e.next)
	//	return
	//} else {
	//	e.exchange()
	//	return 0, false
	//}
}

// 交换前后节点,不能对头部节点调用
func (e *timerTaskEntry)exchange()  {
	if e.pre != nil {
		e.pre.next = e.next
	}
	if e.next != nil {
		e.next.pre = e.pre
	}
}

func (e *timerTaskEntry) isHead() bool {
	if e.pre == nil {
		return false
	}

	// 头部节点pre节点是尾结点
	return e.pre.next == nil
}

// 将传入的entry的数据克隆给自己
func (e *timerTaskEntry) clone(source *timerTaskEntry)  {
	if e.expirationChan != nil {
		close(e.expirationChan)
		e.expirationChan = nil
	}

	if source == nil {
		e.id = 0
		e.pre = nil
		e.next = nil
		e.spec = nil
		e.triggerTime = 0
		e.expirationChan = nil
	} else {
		e.id = source.id
		e.pre = source.pre
		e.next = source.next
		e.spec = source.spec
		e.triggerTime = source.triggerTime
		e.expirationChan = source.expirationChan
	}
}

// 转轮
type overflowWheel struct {
	startNs   int64 // 时间轮开始时间
	tickNs    int64 // tick间隔
	currentNs int64 // 当前时间
	interval  int64 // 转一圈的时间
	buckets   []*timerTaskEntry // 槽位,每个槽的第一人节点是一个空节点,做占位用,以方便删除
	pre       *overflowWheel
	next      *overflowWheel
}

// 时间轮每一次tick需要执行的操作
func (w *overflowWheel) tick(tw *TimeWheel, time *time.Time)  {
	// 如果还不到tick时间,直接返回
	if w.currentNs + w.tickNs > time.UnixNano() {
		// TODO test: cancel tick
		//fmt.Printf(">>> tick cancel, current: %d, tick: %d, now: %d, diff:%d\r\n",
		//	w.currentNs, w.tickNs, time.UnixNano(), w.currentNs + w.tickNs - time.UnixNano())
		return
	}
	// TODO test: do tick
	// fmt.Printf(">>> do tick, current: %d, tick: %d, now: %d\r\n", w.currentNs, w.tickNs, time.UnixNano())

	// 先tick下一级时间轮,因为下一级时间轮有可能有任务需要降级, 降级后可能有任务是可以执行的
	if w.next != nil {
		w.next.tick(tw, time)
	}

	// TODO TEST, 获取旧当前时间
	//old := w.currentNs
	w.updateCurrentNs(time)

	// 执行回调,只有最里面的轮子需要执行
	if w.pre != nil {
		// 如果是次级轮,则将当前游标处节点降级
		node := w.getCurrentNode()
		w.downGrade(tw, time, node)
		return
	} else {
		// TODO TEST-输出执行任务的状态
		//fmt.Printf(">>> exec, current: %d, tick: %d, old: %d, new: %d, now: %d, slot:%d\r\n",
		//	w.currentNs, w.tickNs, old, w.currentNs, time.UnixNano(), w.calculateCurrentBucket())
		// 最里层的时间轮,直接执行
		w.execute(tw, time)
	}
}

// 重新将任务添加到时间轮中,要求任务已经设置好触发时间
func (w *overflowWheel) reAddTaskEntry(entry *timerTaskEntry)  {
	// 先使节点成为独立的节点
	entry.detach()
	w.addTaskEntry(entry)
}

// 将超时任务添加到时间轮
func (w *overflowWheel) addTaskEntry(entry *timerTaskEntry) bool {
	if entry == nil {
		return false
	}

	// 如果触发时间超过了当前时间轮的范围,就要安装给下一级的时间轮,否则直接安装在本时间轮
	//if w.currentNs + w.tickNs > entry.triggerTime && w.pre != nil {
	//	// 是否已经过期了
	//	// 已经过期了
	//	// TODO TEST
	//	diff := w.currentNs + w.tickNs - entry.triggerTime
	//	diffSec := float64(diff) / float64(time.Second)
	//	fmt.Printf("===> expired, current: %d, interval: %d, trigger: %d, diff: %d, diff sec: %f\r\n",
	//		w.currentNs, w.interval, entry.triggerTime, diff, diffSec)
	//	if w.pre != nil {
	//		return w.downGrade(entry)
	//	}

	//	return false
	//} else
	if w.currentNs + w.interval >= entry.triggerTime {
		// 因为每个槽默认装配有一个空节点,所以节点不可能为空
		node := w.getTargetNode(entry.triggerTime)
		// 将新节点和原来的节点合并
		node.concat(entry)
		// TODO TEST: 输出 在interval内添加任务
		//fmt.Printf("===> within interval, current: %d, interval: %d, trigger: %d, tick: %d, cid:%d, tid: %d\r\n",
		//	w.currentNs, w.interval, entry.triggerTime, w.tickNs, w.calculateCurrentBucket(), w.calculateBucketId(entry.triggerTime))
		return true
	} else {
		// 过载了
		// TODO TEST: 过载
		//diff := w.currentNs + w.interval - entry.triggerTime
		//diffSec := float64(diff) / float64(time.Second)
		//fmt.Printf("===> overflow interval, current: %d, interval: %d, trigger: %d, diff: %d, diffSec: %f\r\n",
		//	w.currentNs, w.interval, entry.triggerTime, diff, diffSec)
		return w.overflow(entry)
	}
}

// 获取当前时间对应的槽位的头节点, 因为槽位上默认有一个空节点,所以实际上是返回的这个空节点
func (w *overflowWheel) getCurrentNode() (node *timerTaskEntry) {
	return w.getTargetNode(w.currentNs)
}

func (w *overflowWheel) getTargetNode(expirationTime int64) (entry *timerTaskEntry) {
	bucketId := w.calculateBucketId(expirationTime)
	entry = w.buckets[bucketId]

	return entry
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
		bucketSize := len(w.buckets)
		temp.interval = temp.tickNs * int64(bucketSize)
		temp.pre = w
		temp.next = nil
		temp.initBuckets(bucketSize)
	}
}

func (w *overflowWheel) initBuckets(size int)  {
	if w.buckets == nil {
		w.buckets = make([]*timerTaskEntry, size)
	}

	// 将每个槽位的第一个节点初始化为空节点,且将其pre指向自己, 使其符合任务链的规定
	for i := 0; i < len(w.buckets); i += 1 {
		temp := emptyTaskEntry()
		temp.pre = temp
		w.buckets[i] = temp
	}

}

// 执行超时回调
func (w *overflowWheel) execute(tw *TimeWheel, time *time.Time)  {
	node := w.getCurrentNode()
	w.executeEntry(tw, time, node)
}

func (w *overflowWheel) executeEntry(tw *TimeWheel, time *time.Time, entry *timerTaskEntry)  {
	if entry.id == 0 && entry.next == nil {
		// 没有需要执行的任务
		return
	}

	var enode *timerTaskEntry
	// 断开任务链的头部
	// 处理空节点
	if entry.id == 0 {
		// 取尾节点
		enode = entry.pre
		entry.next.pre = nil
		entry.next = nil
		entry.pre = entry
	} else {
		enode = entry
	}

	// 执行超时任务
	if enode != nil {
		w.executeLink(tw, enode, time)
	}

}

// 链式执行所有任务, 同时添加需要继续执行的任务
func (w *overflowWheel) executeLink(tw *TimeWheel, node *timerTaskEntry, time *time.Time) {
	// 防止因出错导致后续的任务不执行了
	pre := node.pre
	defer func() {
		if pre != nil{
			w.executeLink(tw, pre, time)
		}
	}()

	// 先计算是否需要继续任务
	ifClose := false
	if node.spec.expirationTimeCb != nil {
		// 新的触发时间
		node.triggerTime = uniformNs(node.spec.expirationTimeCb(time), w.tickNs)
		if node.triggerTime > w.currentNs {
			// 还需要继续执行,重新添加
			ifClose = false
			w.reAddTaskEntry(node)
		} else {
			ifClose = true
			node.detach()
			tw.deleteTaskMap(node.id)
		}
	} else {
		ifClose = true
		node.detach()
		tw.deleteTaskMap(node.id)
	}

	// 有回调就调用回调,否则往超时信道里发送超时消息
	if node.spec.callback != nil {
		// TODO 超时任务执行防错
		node.spec.callback(time)
	} else {
		node.expirationChan <- time
		if ifClose {
			close(node.expirationChan)
			node.onCompleteHandler()
			node.expirationChan = nil
		}
	}

}

// 将任务链降级到上一个更精确的时间轮, 本函数不再检查对应的时间轮是否存在
func (w *overflowWheel) downGrade(tw *TimeWheel, now *time.Time, node *timerTaskEntry) (ifDone bool) {
	// 处理空节点
	if node.id == 0 {
		// 如果剩余时间少于了上一级的时间,就可以降级
		if node.next == nil {
			// TODO test: cancel downgrade
			//fmt.Printf(">>> cancel downgrade as next is nil, current: %d, tick: %d, now: %d\r\n", w.currentNs, w.tickNs, 1)
			return false
		}

		// TODO test: exec downgrade
		//fmt.Printf(">>> exec downgrade, current: %d, tick: %d, now: %d\r\n", w.currentNs, w.tickNs, 1)
		// 从后往前处理
		tail := node.pre
		// 将后续节点与空的根节点断开
		node.next.pre = nil
		node.next = nil
		node.pre = node
		return w.downGrade(tw, now, tail)
	} else {
		// TODO 降级未处理
		if w.pre != nil {
			preEntry :=  node.pre
			node.pre = nil
			node.next = nil
			w.pre.downGradeSingle(tw, now, node)
			if preEntry != nil {
				w.downGrade(tw, now, preEntry)
			}
		} else {
			// 已经是最低的时间轮了
			w.executeEntry(tw, now, node)
			return true
		}
	}

	return ifDone
}

// 将单独的节点降级, 由尝试降级到的时间轮调用
func (w *overflowWheel) downGradeSingle(tw *TimeWheel, t *time.Time, entry *timerTaskEntry)  {
	if w.pre == nil {
		w.addTaskEntry(entry)
	} else {
		if entry.triggerTime - w.pre.currentNs < w.pre.interval {
			w.pre.downGradeSingle(tw, t, entry)
		} else {
			w.addTaskEntry(entry)
		}
	}
}

// 更新当前时间
func (w *overflowWheel) updateCurrentNs(t *time.Time)  {
	// TODO TEST update current ns
	//fmt.Printf(">>> update current ms, current: %d, tick: %d, now: %d\r\n", w.currentNs, w.tickNs, t.UnixNano())
	w.currentNs = uniformNs(t.UnixNano(), w.tickNs)
}

// 计算当前时间对应的bucket
func (w *overflowWheel) calculateCurrentBucket() int {
	return w.calculateBucketId(w.currentNs)
}

func (w *overflowWheel) calculateBucketId(ns int64) int {
	return int(ns / w.tickNs % int64(len(w.buckets)))
}

func (w *overflowWheel) dispose()  {
	if w.next != nil {
		w.next.dispose()
	}
	w.pre = nil
	w.next = nil
	w.buckets = nil
}

func uniformNs(ns int64, tickNs int64) int64 {
	return ns - (ns % tickNs)
}

// 默认的超时回调
func defaultTickHandler(time *time.Time, callback ExpirationCallback)  {

}


