package timewheel

import (
	"container/list"
	"sync"
	"time"
)

type Task struct {
	key     string
	delay   time.Duration
	circle  int
	data    interface{}
	element *list.Element
	slot    int // 新增：保存所属 slot，删除时更稳定
}

type TimeWheel struct {
	tick     time.Duration
	slots    []list.List
	slotNum  int
	current  int
	mutex    sync.Mutex
	ticker   *time.Ticker
	taskMap  map[string]*Task
	callback func(data interface{})

	callbackChan chan interface{} // 所有 callback 进入这里
	stopChan     chan struct{}
	onceStart    sync.Once
	onceStop     sync.Once
}

func New(tick time.Duration, slotNum int, callback func(data interface{})) *TimeWheel {
	tw := &TimeWheel{
		tick:         tick,
		slots:        make([]list.List, slotNum),
		slotNum:      slotNum,
		taskMap:      make(map[string]*Task),
		callback:     callback,
		stopChan:     make(chan struct{}),
		callbackChan: make(chan interface{}, 1024),
	}

	tw.startCallbackWorker()
	tw.Start()
	return tw
}

// ==============================
// 启动统一 callback worker 协程
// ==============================
func (tw *TimeWheel) startCallbackWorker() {
	go func() {
		for {
			select {
			case data := <-tw.callbackChan:
				tw.callback(data) // 在唯一 goroutine 中执行
			case <-tw.stopChan:
				return
			}
		}
	}()
}

func (tw *TimeWheel) Start() {
	tw.onceStart.Do(func() {
		tw.ticker = time.NewTicker(tw.tick)
		go func() {
			for {
				select {
				case <-tw.ticker.C:
					tw.tickHandler()
				case <-tw.stopChan:
					tw.ticker.Stop()
					return
				}
			}
		}()
	})
}

func (tw *TimeWheel) Stop() {
	tw.onceStop.Do(func() {
		close(tw.stopChan)
	})
}

// ==============================
// 添加任务（保持外部接口不变）
// ==============================
func (tw *TimeWheel) AddTimer(delay time.Duration, key string, data interface{}) {
	if delay < 0 {
		return
	}

	// ⭐ delay == 0：立即执行，但仍在统一 worker goroutine 中执行
	if delay == 0 {
		select {
		case tw.callbackChan <- data:
		default:
			// 防止 channel 满了阻塞，可根据需要做日志或丢弃策略
			go func() { tw.callbackChan <- data }()
		}
		return
	}

	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	steps := int(delay / tw.tick)
	circle := steps / tw.slotNum
	slot := (tw.current + steps) % tw.slotNum

	task := &Task{
		key:    key,
		delay:  delay,
		circle: circle,
		data:   data,
		slot:   slot,
	}

	e := tw.slots[slot].PushBack(task)
	task.element = e
	tw.taskMap[key] = task
}

// ==============================
// 删除任务（保持外部接口不变）
// ==============================
func (tw *TimeWheel) RemoveTimer(key string) {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	if task, ok := tw.taskMap[key]; ok {
		tw.slots[task.slot].Remove(task.element)
		delete(tw.taskMap, key)
	}
}

// ==============================
// tick handler
// ==============================
func (tw *TimeWheel) tickHandler() {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	slotList := &tw.slots[tw.current]
	var next *list.Element

	for e := slotList.Front(); e != nil; e = next {
		next = e.Next()
		task := e.Value.(*Task)

		if task.circle > 0 {
			task.circle--
			continue
		}

		// 💡 投递到统一 worker，保证同一 goroutine 执行
		tw.callbackChan <- task.data

		slotList.Remove(e)
		delete(tw.taskMap, task.key)
	}

	tw.current = (tw.current + 1) % tw.slotNum
}
