# gozzzworker

[![Go Report Card](https://goreportcard.com/badge/github.com/zondatw/gozzzworker)](https://goreportcard.com/report/github.com/zondatw/gozzzworker)

gozzzworker is Go-based background tasks worker.  

Now:  

* Run worker to execute task
* Specify execution time  
* Supported redis
* Return json message after task finished

Future:  

* Retry task when failed
* Task priority
* Versioning?
* RabbitMQ?

## Installation

To install  
`go get github.com/zondatw/gozzzworker`  

To import  
`import "github.com/zondatw/gozzzworker"`  

## Quickstart

task function need to follow rule:  
```go
func(args json.RawMessage) (interface{}, error)
```

and register function using  
```go
workerObj.RegisterTaskFunction("Task Name", taskFunction)
```

WorkerSetting:  
```go
&gozzzworker.WorkerSetting{
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

	"github.com/zondatw/gozzzworker"
)

func task1(args json.RawMessage) (interface{}, error) {
	type ArgType struct {
		A int    `json:"a"`
		B string `json:"b"`
	}
	var argData ArgType
	json.Unmarshal(args, &argData)
	fmt.Println("Task 1:", argData.A, argData.B)

	type retType struct {
		C int    `json:"c"`
		D string `json:"d"`
	}
	ret := &retType{
		C: 123456,
		D: "Yooooooooooooooooooooooooooooooooooo",
	}
	return ret, nil
}

func task2(args json.RawMessage) (interface{}, error) {
	fmt.Println("Task 2:", args)
	return "", errors.New("yooooooooo")
}

func main() {
	w := gozzzworker.NewWorker(&gozzzworker.WorkerSetting{
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
key: gozzzworker:task:msg
field: 1 (task id need match gozzzworker:task:queue value)
value: '{"task":"Task 1","args":{"a":1,"b":"yoooo"}}' (json format args)

# ZSet
key: gozzzworker:task:queue
value: 1 (task id need match gozzzworker:task:msg field)
score: 123 (timestamp what executed time do you want)
```

example redis command:
```redis
HSET gozzzworker:task:msg 1 '{"task":"Task 1","args":{"a":1,"b":"yoooo"}}'
ZAdd gozzzworker:task:queue 123 1
```

Get return message after execute task
```go
type retMsgType struct {
	Status string `json:"status"` // complete execution
	Msg    string `json:"msg"` // return message json type
}
```
```text
# HASH type
key: gozzzworker:task:ret
field: 1 (task id need match gozzzworker:task:msg field)
```

example redis command:
```redis
HGET gozzzworker:task:ret 1
```

Return message example:
* success
	```json
	{"status": "Success", "msg": {"c":123456,"d":"Yooooooooooooooooooooooooooooooooooo"}}
	```
* fail
	```json
	{"status": "Fail", "msg": {"Error": "yooooooooo"}}
	```

or you can using [gozzzproducer](http://github.com/zondatw/gozzzproducer)  

## Close

You can send following signals to close worker  

* os.Interrupt
* SIGTERM
* SIGINT
* SIGQUIT

## Reference

[dramatiq](https://dramatiq.io/index.html)  
[The Case For A Go Worker Pool](https://brandur.org/go-worker-pool)  