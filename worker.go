package gozzzworker

// Worker is worker struct
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
	broker := NewBroker(pool.AddTask, pool.TaskRetChan, &pool.TaskRetWg, setting.Address, setting.Password, setting.DB)
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
	w.BrokerRun()
}

// BrokerRun will run broker
func (w *Worker) BrokerRun() {
	w.broker.Run()
}
