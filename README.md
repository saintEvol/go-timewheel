# go-timewheel
a go implementation of time wheel

## 使用
### 安装
```go
go get github.com/saintEvol/go-timewheel/timewheel
```

### 代码示例:
```go
package main //
import "github.com/saintEvol/go-timewheel/timewheel"

// 创建一个精度为10毫秒,槽位数量为: 50的时间轮
tw := timewheel.NewTimeWheel(10 * time.Millisecond, 50)
// 启动时间轮
tw.Start()
// 睡眠/等待一秒,将阻塞当前协程
tw.Sleep(1 * time.Second)
// 添加一个1秒后触发,且只执行一次的定时任务
th := tw.Add(timewheel.Once(1 * time.Second))
// 等待任务超时
th.Wait()
// 移除任务
th.Remove()

// 新建一个重复触10次,间隔 100毫秒的定时任务
th1 = timewheel.Add(timewheel.Repeat(10, 100 * time.Millisecond))
for {
	select {
	case _, _ = <- the.Chan()
	    fmt.Print("tick")
    }
}
```