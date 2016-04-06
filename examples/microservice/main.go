package main

import (
	"../../"
	"encoding/json"
	"flag"
	"github.com/golang/glog"
	"io/ioutil"
)

type Config struct {
	futurama.Config
	ServerPort int `json:"server_port"`
}

func NewConfig() *Config {
	return &Config{
		Config:     *(futurama.DefaultConfig()),
		ServerPort: 8765,
	}
}

func main() {
	defer glog.Flush()

	var configFile string = ""
	flag.StringVar(&configFile, "config", "", "The file where config is stored")
	flag.Parse()

	config := loadConfig(configFile)
	if config == nil {
		return
	}

	q, err := futurama.CreateQueue(&config.Config, map[string]futurama.TriggerInterface{
		TriggerType_Http: NewTrigger(),
	})
	if err != nil {
		glog.Errorln("Queue create:", err)
		return
	}
	if err := q.Start(); err != nil {
		glog.Errorln("Queue start:", err)
		return
	}
	defer q.Stop()

	RunHttpServer(config.ServerPort, q)
}

func loadConfig(configFile string) *Config {
	config := NewConfig()

	if configFile != "" {
		file, err := ioutil.ReadFile(configFile)
		if err != nil {
			glog.Errorln("Can not read file", configFile, err)
			return nil
		}
		if err := json.Unmarshal(file, config); err != nil {
			glog.Errorln("Can not parse config", err)
			return nil
		}
	}

	return config
}
