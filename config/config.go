package config

type Config struct {
	Bin2Es     Bin2Es     `toml:"bin2es"`
	MasterInfo MasterInfo `toml:"master_info"`
	Etcd       Etcd       `toml:"etcd"`
	Es         Es         `toml:"es"`
	MysqlSlave Mysql      `toml:"mysql"`
	Sources    []Source   `toml:"source"`
}

type Bin2Es struct {
	SyncChLen int `toml:"sync_ch_len"`
}

type MasterInfo struct {
	Addr          string `toml:"addr"`
	Port          int    `toml:"port"`
	User          string `toml:"user"`
	Pwd           string `toml:"pwd"`
	Charset       string `toml:"charset"`
	Schema        string `toml:"schema"`
	Table         string `toml:"table"`
	FlushDuration int    `toml:"flush_duration"`
}

type Etcd struct {
	Enable      bool     `toml:"enable"`
	EnableTLS   bool     `toml:"enable_tls"`
	LockPath    string   `toml:"lock_path"`
	DialTimeout int      `toml:"dial_timeout"`
	CertPath    string   `toml:"cert_path"`
	KeyPath     string   `toml:"key_path"`
	CaPath      string   `toml:"ca_path"`
	Endpoints   []string `toml:"endpoints"`
}
type Es struct {
	Nodes                []string `toml:"nodes"`
	User                 string   `toml:"user"`
	Passwd               string   `toml:"passwd"`
	EnableAuthentication bool     `toml:"enable_authentication"`
	InsecureSkipVerify   bool     `toml:"insecure_skip_verify"`
	BulkSize             int      `toml:"bulk_size"`
	FlushDuration        int      `toml:"flush_duration"`
}
type Mysql struct {
	Addr     string `toml:"addr"`
	Port     uint64 `toml:"port"`
	User     string `toml:"user"`
	Pwd      string `toml:"pwd"`
	Charset  string `toml:"charset"`
	ServerID uint32 `toml:"server_id"`
}
type Source struct {
	Schema    string   `toml:"schema"`
	DumpTable string   `toml:"dump_table"`
	Tables    []string `toml:"tables"`
}
