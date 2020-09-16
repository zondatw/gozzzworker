package gozzzworker

import "log"

// Wokrer is worker struct
type Worker struct {
	pool   *Pool
	broker *Broker
}

// WorkerSetting is worker setting
type WorkerSetting struct {
	Size     int
	Address  string
	Password string
	DB       int
}

// NewWorker will initialize a new worker
func NewWorker(setting *WorkerSetting) *Worker {
	pool := NewPool(setting.Size)
	broker := NewBroker(pool.AddTask, setting.Address, setting.Password, setting.DB)
	return &Worker{
		pool:   pool,
		broker: broker,
	}
}

// RegisterTaskFunction mapping
func (w *Worker) RegisterTaskFunction(funcName string, function taskFuncType) {
	w.pool.RegisterTaskFunction(funcName, function)
}

// Run worker
func (w *Worker) Run() {
	w.pool.Run()
	go w.PrintErr()
	w.BrokerRun()
}

// PrintErr will print task error
func (w *Worker) PrintErr() {
	for err := range w.pool.ErrorChan {
		log.Println("[Pool error] Err:", err)
		w.pool.ErrWg.Done()
	}
}

// BrokerRun will run broker
func (w *Worker) BrokerRun() {
	w.broker.Run()
}
