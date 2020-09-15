# goZzzWorker

goZzzWorker is Go-based background tasks worker.  

Now:  

* Run worker to execute task
* Specify execution time  
* Supported redis

Future:  

* Return json message after task finished
* Retry task when failed
* Task priority
* Versioning?
* RabbitMQ?

## Installation

To install  
`go get github.com/zondatw/goZzzWorker`  

To import  
`import "github.com/zondatw/goZzzWorker"`  

## Quickstart

task function need to follow rule:  
```go
func(args json.RawMessage) error
```

and register function using  
```go
workerObj.RegisterTaskFunction("Task Name", taskFunction)
```

WorkerSetting:  
```go
&goZzzWorker.WorkerSetting{
    Size:     3,                   // How many concurrent workers do you want
    Address:  "localhost:6379",    // Redis path
    Password: "",                  // Redis password, set empty string if no password 
    DB:       0,                   // Redis DB number
}
```

Example quicker start:  
```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/zondatw/goZzzWorker"
)

func task1(args json.RawMessage) error {
	type ArgType struct {
		A int    `json:"a"`
		B string `json:"b"`
	}
	var argData ArgType
	json.Unmarshal(args, &argData)
	fmt.Println("Task 1:", argData.A, argData.B)
	return nil
}

func task2(args json.RawMessage) error {
	fmt.Println("Task 2:", args)
	return errors.New("yooooooooo")
}

func main() {
	w := goZzzWorker.NewWorker(&goZzzWorker.WorkerSetting{
		Size:     3,
		Address:  "localhost:6379",
        Password: "",
        
		DB:       0,
	})
	w.RegisterTaskFunction("Task 1", task1)
	w.RegisterTaskFunction("Task 2", task2)
	w.RegisterTaskFunction("Task 3", task3)
	w.Run()
}
```

And you can push test data to redis, just follow rule:
```text
# HASH type
key: goZzzWorker:task:msg
field: 1 (task id need match goZzzWorker:task:queue value)
value: '{"task":"Task 1","args":{"a":1,"b":"yoooo"}}' (json format args)

# ZSet
key: goZzzWorker:task:queue
value: 1 (task id need match goZzzWorker:task:msg field)
score: 123 (timestamp what executed time do you want)
```

example redis command:
```redis
HSET goZzzWorker:task:msg 1 '{"task":"Task 1","args":{"a":1,"b":"yoooo"}}'
ZAdd goZzzWorker:task:queue 123 1
```

or you can using [goZzzProducer](http://github.com/zondatw/goZzzProducer)  

## Close

You can send following signals to close worker  

* os.Interrupt
* SIGTERM
* SIGINT
* SIGQUIT

## Reference

[dramatiq](https://dramatiq.io/index.html)  
[The Case For A Go Worker Pool](https://brandur.org/go-worker-pool)  