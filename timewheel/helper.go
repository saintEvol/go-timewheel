package timewheel

import "time"

// 每隔一定间隔定时触发
func Every(duration time.Duration) ExpirationTimeCallback {
	return func(now *time.Time) int64 {
		return now.UnixNano() + int64(duration)
	}
}

// 反复执行指定次数
func Repeat(times int, duration time.Duration) ExpirationTimeCallback {
	return func(time *time.Time) int64 {
		if times > 0 {
			times -= 1
			return time.UnixNano() + int64(duration)
		} else {
			return 0
		}
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


