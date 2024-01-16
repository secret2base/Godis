package timewheel

import (
	"container/list"
	"time"
)

type location struct {
	slot  int           // 任务在时间轮中的槽位
	etask *list.Element // 任务在链表中的位置
}

type task struct {
	delay  time.Duration // 任务延迟时间
	circle int           // 需要等待的完整轮数
	key    string        // 任务标识符
	job    func()        // 任务执行函数
}

type TimeWheel struct {
	interval          time.Duration        // 时间轮的基本时间单元
	ticker            *time.Ticker         // 用于定期触发时间轮的旋转，以驱动任务的执行
	slots             []*list.List         // 一个链表的切片，每个元素是时间轮中一个槽位对应的任务链表
	timer             map[string]*location // 用于快速查找某个任务在时间轮中的位置
	currentPos        int                  // 时间轮当前所处的槽位
	slotNum           int                  // 时间轮总槽位数，决定了时间轮的周期
	addTaskChannel    chan task            // 用于接收新任务的通道，通过该通道将任务添加到时间轮中
	removeTaskChannel chan string          // 用于接收需要移除的任务的通道，通过该通道将指定任务从时间轮中移除
	stopChannel       chan bool            // 用于停止时间轮
}

// New create a new time wheel
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]*location),
		currentPos:        0,
		slotNum:           slotNum,
		addTaskChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	// 创建每个时间槽上对应的链表
	tw.initSlots()

	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// Start starts ticker for time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// start 通过select监听信号
func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(&task)
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	// 找到当前时间槽对应的链表
	l := tw.slots[tw.currentPos]
	// 时间轮步进
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
	go tw.scanAndRunTask(l)
}

func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle
	// 在给定位置的链表上增加任务，返回增加的元素
	e := tw.slots[pos].PushBack(task)
	loc := &location{
		slot:  pos,
		etask: e,
	}
	if task.key != "" {
		_, ok := tw.timer[task.key]
		// 移除同名任务
		if ok {
			tw.removeTask(task.key)
		}
	}
	tw.timer[task.key] = loc
}

func (tw *TimeWheel) removeTask(key string) {
	// 通过key在时间槽及timer中移除此任务
	pos, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[pos.slot]
	l.Remove(pos.etask)
	delete(tw.timer, key)
}

func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	for e := l.Front(); e != nil; {
		// 通过类型断言将e.Value转化为task类型
		task := e.Value.(*task)
		// 判断此任务是否应在当前轮执行
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		// 如果定时任务到时间，则开启协程执行
		go func() {
			defer func() {
				if err := recover(); err != any(nil) {
					// logger.Error(err)
				}
			}()
			job := task.job
			job()
		}()
		next := e.Next()
		l.Remove(e)
		if task.key != "" {
			delete(tw.timer, task.key)
		}
		e = next
	}
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = delaySeconds / intervalSeconds / tw.slotNum
	pos = (tw.currentPos + delaySeconds/intervalSeconds) % tw.slotNum

	return
}

// Stop stops the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob add new job into pending queue
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{delay: delay, key: key, job: job}
}

// RemoveJob add remove job from pending queue
// if job is done or not found, then nothing happened
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}
