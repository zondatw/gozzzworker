package gozzzworker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// taskRetData is return data struct for taskRetChan
type taskRetData struct {
	taskID string
	retMsg string
}

// taskMsgType is return message json schema
type retMsgType struct {
	Status string `json:"status"` // complete execution
	Msg    string `json:"msg"`    // return message json type
}

// Pool is worker pool struct
type Pool struct {
	size          int
	mux           sync.Mutex
	taskSetting   *TaskSetting
	priorityQueue []*Task
	taskChan      chan *Task
	taskWg        sync.WaitGroup
	TaskRetChan   chan *taskRetData
	TaskRetWg     sync.WaitGroup
}

// NewPool will initialize a new pool
func NewPool(size int) *Pool {
	return &Pool{
		size:          size,
		priorityQueue: make([]*Task, 0),
		taskSetting:   NewTaskSetting(),
		taskChan:      make(chan *Task),
		TaskRetChan:   make(chan *taskRetData),
	}
}

// RegisterTaskFunction mapping
func (p *Pool) RegisterTaskFunction(funcName string, function taskFuncType) {
	p.taskSetting.Register(funcName, function)
}

// Run create pool to run all work
func (p *Pool) Run() {
	p.setupCloseHandler()
	for i := 0; i < p.size; i++ {
		go p.worker()
	}
	go p.dispatcher()
}

// AddTask to task chan
func (p *Pool) AddTask(taskID string, funcName string, args json.RawMessage, priority int) {
	p.taskWg.Add(1)
	log.Printf("[Pool AddTask] taskID %s (%d)", taskID, priority)
	p.mux.Lock()
	defer p.mux.Unlock()
	p.priorityQueue = append(
		p.priorityQueue,
		NewTask(
			taskID,
			p.taskSetting.funcMap[funcName],
			args,
		),
	)
}

func (p *Pool) dispatcher() {
	for {
		for {
			p.mux.Lock()
			log.Println(len(p.priorityQueue))
			if len(p.priorityQueue) == 0 {
				p.mux.Unlock()
				break
			}
			p.passTaskToChan()
			p.mux.Unlock()
		}

		time.Sleep(1 * time.Second)
	}
}

func (p *Pool) passTaskToChan() {
	log.Printf("[Pool passTaskToChan] taskID %s", p.priorityQueue[0].id)
	p.taskChan <- p.priorityQueue[0]
	p.priorityQueue = p.priorityQueue[1:]
}

func (p *Pool) worker() {
	for task := range p.taskChan {
		var retJSONStr string
		status := "Fail"
		retMsg, err := task.Run(&p.taskWg)
		if err == nil {
			retJSONByteArrayData, err := json.Marshal(retMsg)
			if err != nil {
				retJSONStr = fmt.Sprintf(`{"Error": "Marchal json field: %s"}`, err.Error())
			} else {
				status = "Success"
				retJSONStr = string(retJSONByteArrayData)
			}
		} else {
			retJSONStr = fmt.Sprintf(`{"Error": "%s"}`, err.Error())
		}
		p.TaskRetWg.Add(1)
		retMsgByteArray, err := json.Marshal(&retMsgType{
			Status: status,
			Msg:    retJSONStr,
		})
		p.TaskRetChan <- &taskRetData{
			taskID: task.id,
			retMsg: string(retMsgByteArray),
		}
	}
}

// setupCloseHandler safely close when get TERM, INT, QUIT interrupt
func (p *Pool) setupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	go func() {
		for sig := range c {
			log.Printf("[Pool setupCloseHandler] Captured %v", sig)
			p.End()
			os.Exit(0)
		}
	}()
}

// End safely close chan
func (p *Pool) End() {
	log.Println("[End] Close task channel")
	close(p.taskChan)
	p.taskWg.Wait()
	log.Println("[End] Close task ret channel")
	close(p.TaskRetChan)
	p.TaskRetWg.Wait()
	log.Println("[End] Close finish")
}
