package err

import "errors"

var (
	NoFileError = errors.New("请传入配置文件")
)
