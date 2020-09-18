package gozzzworker

import (
	"encoding/json"
	"sync"
)

type taskFuncType func(args json.RawMessage) (interface{}, error)

// TaskSetting is a setting about task function mapping
type TaskSetting struct {
	funcMap map[string]taskFuncType
}

// Register function mapping
func (ts *TaskSetting) Register(funcName string, function taskFuncType) {
	ts.funcMap[funcName] = function
}

// NewTaskSetting create new task setting
func NewTaskSetting() *TaskSetting {
	return &TaskSetting{funcMap: make(map[string]taskFuncType)}
}

// Task struct
type Task struct {
	id       string
	function taskFuncType
	args     json.RawMessage
}

// NewTask create new task
func NewTask(id string, function taskFuncType, args json.RawMessage) *Task {
	return &Task{id: id, function: function, args: args}
}

// Run task function
func (t *Task) Run(wg *sync.WaitGroup) (retMsg interface{}, err error) {
	retMsg, err = t.function(t.args)
	wg.Done()
	return
}
