package gozzzworker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type taskRetData struct {
	taskID string
	retMsg string
}

// Pool is worker pool struct
type Pool struct {
	size        int
	taskSetting *TaskSetting
	taskChan    chan *Task
	taskWg      sync.WaitGroup
	TaskRetChan chan *taskRetData
	TaskRetWg   sync.WaitGroup
}

// NewPool will initialize a new pool
func NewPool(size int) *Pool {
	return &Pool{
		size:        size,
		taskSetting: NewTaskSetting(),
		taskChan:    make(chan *Task),
		TaskRetChan: make(chan *taskRetData),
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
}

// AddTask to task chan
func (p *Pool) AddTask(taskID string, funcName string, args json.RawMessage) {
	p.taskWg.Add(1)
	p.taskChan <- NewTask(
		taskID,
		p.taskSetting.funcMap[funcName],
		args,
	)
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
		p.TaskRetChan <- &taskRetData{
			taskID: task.id,
			retMsg: fmt.Sprintf(`{"status": "%s", "msg": %s}`, status, retJSONStr),
		}
	}
}

// setupCloseHandler safely close when get TERM, INT, QUIT interrupt
func (p *Pool) setupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	go func() {
		for sig := range c {
			log.Printf("[setupCloseHandler] Captured %v", sig)
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
