package main

import (
	"../../"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"net/http"
	"time"
)

const (
	TriggerType_Http = "http"
)

type Trigger struct {
	client *http.Client
}

func NewTrigger() *Trigger {
	return &Trigger{
		client: &http.Client{},
	}
}

func (self *Trigger) Trigger(ev *futurama.Event) *futurama.TriggerResult {
	param := ev.Data.(map[string]interface{})

	host := param["host"].(string)
	port, _ := param["port"].(json.Number).Int64()
	path := param["path"].(string)
	url := fmt.Sprintf("http://%s:%d%s", host, port, path)
	postData, _ := futurama.Encoder.Marshal(param["data"])

	glog.Infoln("Post to downstream url", url)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(postData))

	result := &futurama.TriggerResult{futurama.EventStatus_RETRY, time.Time{}, nil}
	if resp, err := self.client.Do(req); err != nil {
		glog.Errorf("Sending request, err: %s id: %s", err, ev.Id)
		result.Data = err
	} else {
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			result.Status = futurama.EventStatus_OK
		} else {
			glog.Infoln("Retry on StatusCode =", resp.StatusCode)
		}
	}
	return result
}
