package gozzzworker

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// Broker is broker struct
type Broker struct {
	conn        *RedisConn
	addTask     func(string, string, json.RawMessage, int)
	queueKey    string // queue key on redis
	msgKey      string // msg key on redis
	retKey      string // ret key on redis
	taskRetChan <-chan *taskRetData
	taskRetWg   *sync.WaitGroup
}

// TaskJSONType is register task json schema
type TaskJSONType struct {
	Task string          `json:"task"`
	Args json.RawMessage `json:"args"`
	Priority int         `json:"priority"`
}

// NewBroker will initialize a new broker
func NewBroker(addTask func(string, string, json.RawMessage, int), taskRetChan <-chan *taskRetData, taskRetWg *sync.WaitGroup, address string, password string, db int) *Broker {
	return &Broker{
		addTask:     addTask,
		conn:        NewRedisConn(address, password, db),
		queueKey:    "gozzzworker:task:queue",
		msgKey:      "gozzzworker:task:msg",
		retKey:      "gozzzworker:task:ret",
		taskRetChan: taskRetChan,
		taskRetWg:   taskRetWg,
	}
}

// Run get due tasks
func (b *Broker) Run() {
	go b.storeTasksRetData()
	for {
		now := fmt.Sprintf("%d", time.Now().Unix())
		taskIDArray, err := b.conn.GetZRangeByScoreLessThan(b.queueKey, now)
		if err != nil {
			log.Println("[broker] Err:", err)
		} else {
			log.Println("[broker] Get tasks id:", taskIDArray)
			b.conn.RemoveZSet(b.queueKey, taskIDArray)
			go b.addTasks(taskIDArray)
		}
		time.Sleep(1 * time.Second)
	}
}

// addTask parse tasks msg and add those to worker
func (b *Broker) addTasks(taskIDArray []string) {
	log.Println("[broker addTasks] Get tasks id:", taskIDArray)
	for _, taskID := range taskIDArray {
		log.Println("[broker addTasks] task id:", taskID)
		msg, err := b.conn.GetHashValue(b.msgKey, taskID)
		if err != nil {
			log.Println("[broker addTasks] Err:", err)
		} else {
			var jsonData TaskJSONType = TaskJSONType{
				Priority: 0,
			}
			json.Unmarshal([]byte(msg), &jsonData)
			b.addTask(taskID, jsonData.Task, jsonData.Args, jsonData.Priority)
		}
		b.conn.RemoveHash(b.msgKey, taskID)
	}
}

// storeTasksRetData store tasks return data to redis
func (b *Broker) storeTasksRetData() {
	for taskRet := range b.taskRetChan {
		b.conn.SetHashValue(
			b.retKey, taskRet.taskID, taskRet.retMsg,
		)
		b.taskRetWg.Done()
	}
}
