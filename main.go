package main

import (
	"context"
	"fmt"
	"time"
)

type Payload struct{}

// WORKERS
type Worker struct {
	tasks chan Payload
}

func NewWorker(tasks chan Payload) (*Worker, error) {
	return &Worker{
		tasks: tasks,
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
}

func NewManager() (*Manager, error) {
	return &Manager{
		workerCalls:     0,
		workerPoolSize:  5, // TODO: pass config
		workerQueueSize: 2, // TODO: pass config
	}, nil
}

func (m *Manager) Start(ctx context.Context) {
	m.workers = make([]*Worker, m.workerPoolSize)

	for i := 0; i < m.workerPoolSize; i++ {
		tasks := make(chan Payload, m.workerQueueSize)
		w, err := NewWorker(tasks)
		if err != nil {
			fmt.Println("worker init error")
		}
		m.workers[i] = w
		m.workers[i].Start(ctx)
	}
}

func (m *Manager) AddJob() error {
	index := m.workerCalls % uint32(len(m.workers))
	m.workerCalls++

	q := m.workers[index].GetMessageQueue()
	p := Payload{}
	q <- p

	return nil
}

func main() {
	m, err := NewManager()
	if err != nil {
		fmt.Println("Error while creating manager")
	}

	go func() {
		m.Start(context.Background())
	}()

	// Ideally, we wouldn't call the AddJob so quickly, and this is not an issue
	// So, we are sleep here for a bit.
	time.Sleep(1 * time.Second)

	for _ = range 10 {
		m.AddJob()
	}

	time.Sleep(20 * time.Second)
}
