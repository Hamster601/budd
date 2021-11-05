package datefortmat

import (
	"strings"
	"time"
)

const (
	// 日期格式
	DayFormatter = "2006-01-02"
	// 日期时间格式--分
	DayTimeMinuteFormatter = "2006-01-02 15:04"
	// 日期时间格式--秒
	DayTimeSecondFormatter = "2006-01-02 15:04:05"
	// 日期时间格式--毫秒
	DayTimeMillisecondFormatter = "2006-01-02 15:04:05.sss"
	// 时间格式--秒
	TimeSecondFormatter = "15:04:05"
)

// 默认格式2006-01-02 15:04:05
func NowFormatted() string {
	return time.Now().Format(DayTimeSecondFormatter)
}

func NowLayout(layout string) string {
	return time.Now().Format(layout)
}

func Layout(date time.Time, layout string) string {
	return date.Format(layout)
}

func DefaultLayout(time time.Time) string {
	return time.Format(DayTimeSecondFormatter)
}

func FromDefaultLayout(str string) time.Time {
	loc, _ := time.LoadLocation("Local")
	theTime, _ := time.ParseInLocation(DayTimeSecondFormatter, str, loc)
	return theTime
}

// 当前的毫秒时间戳
func NowMillisecond() int64 {
	return time.Now().UnixNano() / 1e6
}

func PastDayDate(pastDay int) time.Time {
	return time.Now().AddDate(0, 0, -pastDay)
}

func FutureDayDate(futureDay int) time.Time {
	return time.Now().AddDate(0, 0, futureDay)
}

func WeekStartDayDate() time.Time {
	now := time.Now()
	offset := int(time.Monday - now.Weekday())
	if offset > 0 {
		offset = -6
	}
	weekStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, offset)
	return weekStart
}

func MonthStartDayDate() time.Time {
	year, month, _ := time.Now().Date()
	monthStart := time.Date(year, month, 1, 0, 0, 0, 0, time.Local)
	return monthStart
}

func ConvertGoFormat(format string) string {
	var goFormate = format
	if strings.Contains(goFormate, "YYYY") {
		goFormate = strings.Replace(goFormate, "YYYY", yyyy, -1)
	} else if strings.Contains(goFormate, "yyyy") {
		goFormate = strings.Replace(goFormate, "yyyy", yyyy, -1)
	} else if strings.Contains(goFormate, "YY") {
		goFormate = strings.Replace(goFormate, "YY", yy, -1)
	} else if strings.Contains(goFormate, "yy") {
		goFormate = strings.Replace(goFormate, "yy", yy, -1)
	}

	if strings.Contains(goFormate, "MMMM") {
		goFormate = strings.Replace(goFormate, "MMMM", mmmm, -1)
	} else if strings.Contains(goFormate, "mmmm") {
		goFormate = strings.Replace(goFormate, "mmmm", mmmm, -1)
	} else if strings.Contains(goFormate, "MMM") {
		goFormate = strings.Replace(goFormate, "MMM", mmm, -1)
	} else if strings.Contains(goFormate, "mmm") {
		goFormate = strings.Replace(goFormate, "mmm", mmm, -1)
	} else if strings.Contains(goFormate, "mm") {
		goFormate = strings.Replace(goFormate, "mm", mm, -1)
	}

	if strings.Contains(goFormate, "dddd") {
		goFormate = strings.Replace(goFormate, "dddd", dddd, -1)
	} else if strings.Contains(goFormate, "ddd") {
		goFormate = strings.Replace(goFormate, "ddd", ddd, -1)
	} else if strings.Contains(goFormate, "dd") {
		goFormate = strings.Replace(goFormate, "dd", dd, -1)
	}

	if strings.Contains(goFormate, "tt") {
		if strings.Contains(goFormate, "HH") {
			goFormate = strings.Replace(goFormate, "HH", HHT, -1)
		} else if strings.Contains(goFormate, "hh") {
			goFormate = strings.Replace(goFormate, "hh", HHT, -1)
		}
		goFormate = strings.Replace(goFormate, "tt", tt, -1)
	} else {
		if strings.Contains(goFormate, "HH") {
			goFormate = strings.Replace(goFormate, "HH", HH, -1)
		} else if strings.Contains(goFormate, "hh") {
			goFormate = strings.Replace(goFormate, "hh", HH, -1)
		}
		goFormate = strings.Replace(goFormate, "tt", "", -1)
	}

	if strings.Contains(goFormate, "MM") {
		goFormate = strings.Replace(goFormate, "MM", MM, -1)
	}

	if strings.Contains(goFormate, "SS") {
		goFormate = strings.Replace(goFormate, "SS", SS, -1)
	} else if strings.Contains(goFormate, "ss") {
		goFormate = strings.Replace(goFormate, "ss", SS, -1)
	}

	if strings.Contains(goFormate, "ZZZ") {
		goFormate = strings.Replace(goFormate, "ZZZ", ZZZ, -1)
	} else if strings.Contains(goFormate, "zzz") {
		goFormate = strings.Replace(goFormate, "zzz", ZZZ, -1)
	} else if strings.Contains(goFormate, "Z") {
		goFormate = strings.Replace(goFormate, "Z", Z, -1)
	} else if strings.Contains(goFormate, "z") {
		goFormate = strings.Replace(goFormate, "z", Z, -1)
	}

	if strings.Contains(goFormate, "tt") {
		goFormate = strings.Replace(goFormate, "tt", tt, -1)
	}
	if strings.Contains(goFormate, "o") {
		goFormate = strings.Replace(goFormate, "o", o, -1)
	}

	return goFormate
}
