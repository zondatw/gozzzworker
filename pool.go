package gozzzworker

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Pool is worker pool struct
type Pool struct {
	size        int
	taskSetting *TaskSetting
	taskChan    chan *Task
	ErrorChan   chan error
	wg          sync.WaitGroup
	ErrWg       sync.WaitGroup
}

// NewPool will initialize a new pool
func NewPool(size int) *Pool {
	return &Pool{
		size:        size,
		taskSetting: NewTaskSetting(),
		taskChan:    make(chan *Task),
		ErrorChan:   make(chan error),
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
func (p *Pool) AddTask(funcName string, args json.RawMessage) {
	p.wg.Add(1)
	p.taskChan <- NewTask(
		p.taskSetting.funcMap[funcName],
		args,
	)
}

func (p *Pool) worker() {
	for task := range p.taskChan {
		err := task.Run(&p.wg)
		if err != nil {
			p.ErrWg.Add(1)
			p.ErrorChan <- err
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
	p.wg.Wait()
	log.Println("[End] Close error channel")
	close(p.ErrorChan)
	p.ErrWg.Wait()
	log.Println("[End] Close finish")
}
