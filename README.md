# Futurama

Futurama is a mysql backed priority queue for scheduling delayed events, written in Go.

* It allows you to execute events at specified time in future.
* Different from background batch job targeted job queues, it is good at handling simple/small tasks (e.g. calling a http API after 30sec) under heavy production load.
* It has a stateless design so that you can run multiple instances for load balancing or failover.
* It is empowering one of top ranked MMO strategy game - [War of Nations](https://itunes.apple.com/us/app/war-nations-pvp-strategy-mmo/id568212992).
* It is supposed to support multiple backends for storing events, for the time being only MySQL is officially implemented.

## Requirements

Well tested with:
 - Go >= 1.3
 - MySQL >= 5.5

## Basic usage

```go
// create a queue
q := futurama.CreateQueue(config, map[string]TriggerInterface{
      triggerType: trigger,
      ...
})
q.Start()

// schedule an event
triggerTime := time.Now().Add(3 * time.Second)
evId := q.Create(triggerType, triggerTime, triggerParam)

// cancel the event
if we_want_to_cancel {
  q.Cancel(evId)
}

// stop the queue
q.Stop()
```

* Create an event queue with ```futurama.CreateQueue```.
  * 1st arg ```config``` provides access to MySQL .. etc.
  * 2nd arg provides triggers with a map of string -> TriggerInterface, these triggers are used to process events.
* Calling ```q.Create()``` with ```triggerType```(string), ```triggerTime```(time.Time) and ```triggerParams```(interface{}) will add an event to queue.
  * Later at ```triggerTime```, a trigger associated with ```triggerType``` will be called.
* ```q.Create()``` returns the id(string) of created event, id can be use for cancelling the event.

### Config

By default, futurama connects to local MySQL server (host=127.0.0.1, port=3306). You can either create a default config and replace with customized values:

```go
config := futurama.DefaultConfig()
config.Host = "example.com"
config.User = "dev"
config.Pass = "..."

q := futurama.CreateQueue(config, triggers)
...
```

or load config values from a json file - a ```Config``` object is json deserializable:

```go
configFile := "queue.json"
config := futurama.DefaultConfig()
file, _ := ioutil.ReadFile(configFile)
json.Unmarshal(file, config)

q := futurama.CreateQueue(config, triggers)
...
```

inside ```queue.json```:

```json
{
  "mysql6": false,
  "username": "dev",
  "password": "...",
  "host": "example.com"
} // see config.go for more setting options 
```

*NOTE*: By enabling ```mysql6```, scheduled time can be specified in millisecond (and futurama needs to actually connect to a MySQL server that supports ```DATETIME(6)```) 

### Triggers

A trigger can be any go struct that implements ```TriggerInterface``` (see interface.go)

```go
type TriggerResult struct {
	Status      EventStatus
	TriggerTime time.Time
	Data        interface{}
}

type TriggerInterface interface {
 Trigger(ev *Event) *TriggerResult
}
```

* ```ev *Event``` is created by ```q.Create(triggerType, triggerTime, triggerParam)```
* ```Trigger``` function is called at ```triggerTime```, it can access ```triggerParam``` through ```ev.Data```

## Retry on failures(Backoff)

* Events will be re-scheduled if ```Trigger``` function failed (return ``TriggerResult.Status = EventStatus_RETRY```)
* Re-scheduled triggerTime is delayed upon failures by following exponential backoff
* Max number of re-attempts is 18 by default, it can be configured by ```Config.SchedulerConfig.MaxRetry```, after ```MaxRetry```, the event will be forgotten (removed from DB) ...

## Running test

```
go test -v -logtostderr
```

