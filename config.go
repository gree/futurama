package futurama

type Config struct {
	StatIntervalSec int `json:"stat_interval_sec"`
	SchedulerConfig
	MySQLConfig
}

type SchedulerConfig struct {
	MaxScheduledEvents int `json:"max_scheduled_events"`
	MaxRetry           int `json:"max_retry"`
}

type MySQLConfig struct {
	MySQL6            bool   `json:"mysql6"`
	User              string `json:"username"`
	Pass              string `json:"password"`
	Host              string `json:"host"`
	Port              int    `json:"port"`
	DbName            string `json:"db_name"`
	TableName         string `json:"table_name"`
	MaxOpenConnection int    `json:"max_open_connection"`

	ConsumerName           string `json:"queue_name"`
	ConsumerLockTimeoutSec int    `json:"consumer_lock_timeout_sec"`
	ConsumerTimeWindowSec  int    `json:"consumer_time_window_sec"`
	ConsumerSelectLimit    int    `json:"consumer_select_limit"`
	ConsumerSleepMSec      int    `json:"consumer_sleep_msec"`
}

func DefaultConfig() *Config {
	return &Config{
		StatIntervalSec: 0,
		SchedulerConfig: SchedulerConfig{
			MaxScheduledEvents: 10000,
			MaxRetry:           18,
		},
		MySQLConfig: MySQLConfig{
			MySQL6:            true,
			User:              "root",
			Pass:              "",
			Host:              "127.0.0.1",
			Port:              3306,
			DbName:            "futurama",
			TableName:         "events",
			MaxOpenConnection: 10,

			ConsumerName:           "",
			ConsumerLockTimeoutSec: 31,
			ConsumerTimeWindowSec:  5,
			ConsumerSelectLimit:    50,
			ConsumerSleepMSec:      100,
		},
	}
}
