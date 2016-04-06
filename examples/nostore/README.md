This example demonstrates what is needed to write your own event store for futurama.

## Interfaces

Definition of interfaces can be found in [interfaces.go](/interfaces.go). To support other backends (redis, mongodb, ..etc), you need to:

* Implement ```StoreInterface``` and ```ConsumerInterface```
* Create queue with ```futurama.CreateCustomQueue```
* Provide store and consumer instances to ```Populate``` method

```go
noConsumer := &NoConsumer{make(chan []*futurama.Event, 1024)}
noStore    := &NoStore{noConsumer.C}
q, err := futurama.CreateCustomQueue(config, triggers).Populate(noStore, noConsumer)
if err != nil {
   ...
}

q.start()

...
```


## The example

```nostore``` has minimum implementation of ```StoreInterface``` and ```ConsumerInterface```, it just bypass the events to channels, everything is kept in memory

## Build & run

```
$ go get
$ go build
$ ./nostore
```

