package goZzzWorker

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// Broker is broker struct
type Broker struct {
	conn     *RedisConn
	addTask  func(string, json.RawMessage)
	queueKey string
	msgKey   string
}

// TaskJsonType is register task json schema
type TaskJsonType struct {
	Task string          `json:"task"`
	Args json.RawMessage `json:"args"`
}

// NewBroker will initialize a new broker
func NewBroker(addTask func(string, json.RawMessage), address string, password string, db int) *Broker {
	return &Broker{
		addTask:  addTask,
		conn:     NewRedisConn(address, password, db),
		queueKey: "goZzzWorker:task:queue",
		msgKey:   "goZzzWorker:task:msg",
	}
}

// Run get due tasks
func (b *Broker) Run() {
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
	log.Println("[addTasks] Get tasks id:", taskIDArray)
	for _, taskID := range taskIDArray {
		log.Println("[addTasks] task id:", taskID)
		msg, err := b.conn.GetHashValue(b.msgKey, taskID)
		if err != nil {
			log.Println("[addTasks] Err:", err)
		} else {
			var jsonData TaskJsonType
			json.Unmarshal([]byte(msg), &jsonData)
			b.addTask(jsonData.Task, jsonData.Args)
		}
		b.conn.RemoveHash(b.msgKey, taskID)
	}
}
