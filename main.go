package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Payload struct{}

// WORKERS
type Worker struct {
	tasks chan Payload
	wg    *sync.WaitGroup
}

func NewWorker(tasks chan Payload, wg *sync.WaitGroup) (*Worker, error) {
	return &Worker{
		tasks: tasks,
		wg:    wg,
	}, nil
}

func (w *Worker) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case payload := <-w.tasks:
				err := w.onTaskReceived(ctx, payload)
				if err != nil {
					fmt.Println("Worker task hander error ")
				}
				w.wg.Done()
			case <-ctx.Done():
				return
			}
		}
	}()
	// Log start
	fmt.Println("worker started")
}

func (w *Worker) GetMessageQueue() chan Payload {
	return w.tasks
}

func (w *Worker) onTaskReceived(ctx context.Context, payload Payload) error {
	time.Sleep(2 * time.Second)
	fmt.Println("Dummy expensive task finished")
	return nil
}

// MANAGER
type Manager struct {
	workers     []*Worker
	workerCalls uint32

	workerPoolSize  int
	workerQueueSize int

	wg *sync.WaitGroup
}

func NewManager(wg *sync.WaitGroup) (*Manager, error) {
	return &Manager{
		workerCalls:     0,
		workerPoolSize:  5, // TODO: pass config
		workerQueueSize: 2, // TODO: pass config
		wg:              wg,
	}, nil
}

func (m *Manager) Start(ctx context.Context) {
	m.workers = make([]*Worker, m.workerPoolSize)

	for i := 0; i < m.workerPoolSize; i++ {
		tasks := make(chan Payload, m.workerQueueSize)
		w, err := NewWorker(tasks, m.wg)
		if err != nil {
			fmt.Println("worker init error")
		}
		m.workers[i] = w
		m.workers[i].Start(ctx)
	}
}

func (m *Manager) AddJob() error {
	m.wg.Add(1)

	index := m.workerCalls % uint32(len(m.workers))
	m.workerCalls++

	q := m.workers[index].GetMessageQueue()
	p := Payload{}
	q <- p

	return nil
}

func main() {
	var wg sync.WaitGroup

	m, err := NewManager(&wg)
	if err != nil {
		fmt.Println("Error while creating manager")
	}

	m.Start(context.Background())

	for _ = range 10 {
		m.AddJob()
	}

	wg.Wait()

}
