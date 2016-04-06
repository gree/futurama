Here contains an example to show how to build a http based event queue service with futurama.

* ```microservice``` allows you to schedule a http call to any url at specified time.
* By posting below json to ```/add``` api, ```microservice``` will post ```data``` to ```http://127.0.0.1:8765/downstream/call``` at ```$UNIX_TIMESTAMP```

```
{
  "trigger_time": $UNIX_TIMESTAMP,
  "trigger_type": "http",
  "host": 127.0.0.1,
  "port": 8765,
  "path": "/downstream/call",
  "data": {
    "a": 1,
    "b": "x"
  }
}
```

## Build

```
$ go get
$ go build
```

compiled binary ```microservice``` should be found in this directory.

## Start server & Test

```
$ ./microservice --config ./dev.json --logtostderr &
$ ./schedule_event.sh 8765
```

```8765``` is the default port used by ```microservice```

