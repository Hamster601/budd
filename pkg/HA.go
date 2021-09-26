package pkg

/* 高可用接口
 * 可自定义扩展
 */
type HA interface {
	Lock() error
	UnLock() error
	Close()
}
