package main

import (
	"../../"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"net/http"
	"time"
)

type HttpServer struct {
	queue *futurama.Queue
}

func RunHttpServer(port int, queue *futurama.Queue) {
	s := &HttpServer{queue}
	http.HandleFunc("/add", s.add)
	http.HandleFunc("/remove", s.remove)
	http.HandleFunc("/stat", s.stat)
	http.HandleFunc("/downstream/call", s.downstreamCall)
	glog.Infoln("Listen on port", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func (self *HttpServer) add(w http.ResponseWriter, r *http.Request) {
	data := make(map[string]interface{})
	d := json.NewDecoder(r.Body)
	d.UseNumber()
	if err := d.Decode(&data); err != nil {
		respondError(w, err.Error(), http.StatusOK)
		return
	}

	triggerType := data["trigger_type"].(string)
	triggerTimestamp, _ := data["trigger_time"].(json.Number).Int64()
	triggerTime := time.Unix(triggerTimestamp, 0)
	if id := self.queue.Create(triggerType, triggerTime, data); id == "" {
		respondError(w, "Can not create event", http.StatusOK)
		return
	} else {
		body, _ := futurama.Encoder.Marshal(struct {
			A string `json:"event_id"`
		}{id})
		w.Write(body)

		glog.Infoln("Created event", id)
	}
}

func (self *HttpServer) remove(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	queryId, ok := query["event_id"]
	if !ok {
		respondError(w, "event_id is requried", http.StatusOK)
		return
	} else {
		if len(queryId) == 0 || queryId[0] == "" {
			respondError(w, "event_id is empty", http.StatusOK)
			return
		}
	}

	id := queryId[0]
	glog.Infof("Cancel event id: %s", id)

	if err := self.queue.Cancel(id); err != nil {
		respondError(w, "Can not cancel event", http.StatusOK)
		return
	}

	body, _ := futurama.Encoder.Marshal(struct {
		A bool `json:"status"`
	}{true})
	w.Write(body)
}

func (self *HttpServer) stat(w http.ResponseWriter, r *http.Request) {
	body, _ := futurama.Encoder.Marshal(self.queue.GetStat())
	w.Write(body)
}

func (self *HttpServer) downstreamCall(w http.ResponseWriter, r *http.Request) {
	glog.Infoln("downstreamCall, event triggered")
	w.Write([]byte("{}"))
}

func respondError(w http.ResponseWriter, msg string, statusCode int) {
	glog.InfoDepth(1, msg)
	body, _ := futurama.Encoder.Marshal(struct {
		bool   `json:"status"`
		string `json:"error,omitempty"`
	}{false, msg})
	w.WriteHeader(statusCode)
	w.Write(body)
}
