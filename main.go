package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
)

type Payload struct{}

// WORKERS
type Worker struct {
	tasks chan Payload
	wg    *sync.WaitGroup
	pb    *progressbar.ProgressBar
}

func NewWorker(tasks chan Payload, wg *sync.WaitGroup, pb *progressbar.ProgressBar) (*Worker, error) {
	return &Worker{
		tasks: tasks,
		wg:    wg,
		pb:    pb,
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
				w.pb.Add(1)
			case <-ctx.Done():
				return
			}
		}
	}()
	// Log start
	//fmt.Println("worker started")
}

func (w *Worker) GetMessageQueue() chan Payload {
	return w.tasks
}

func (w *Worker) onTaskReceived(ctx context.Context, payload Payload) error {
	offset := rand.IntN(1500)
	waitTime := time.Duration(offset + 500)
	time.Sleep(waitTime * time.Millisecond)
	//fmt.Println("Dummy expensive task finished")
	return nil
}

// MANAGER
type Manager struct {
	workers     []*Worker
	workerCalls uint32

	workerPoolSize  int
	workerQueueSize int

	wg *sync.WaitGroup
	pb *progressbar.ProgressBar
}

func NewManager(wg *sync.WaitGroup, pb *progressbar.ProgressBar) (*Manager, error) {
	return &Manager{
		workerCalls:     0,
		workerPoolSize:  5, // TODO: pass config
		workerQueueSize: 2, // TODO: pass config
		wg:              wg,
		pb:              pb,
	}, nil
}

func (m *Manager) Start(ctx context.Context) {
	m.workers = make([]*Worker, m.workerPoolSize)

	for i := 0; i < m.workerPoolSize; i++ {
		tasks := make(chan Payload, m.workerQueueSize)
		w, err := NewWorker(tasks, m.wg, m.pb)
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

	numberOfTasks := 10
	pb := progressbar.Default(int64(numberOfTasks))

	m, err := NewManager(&wg, pb)
	if err != nil {
		fmt.Println("Error while creating manager")
	}

	m.Start(context.Background())

	for _ = range numberOfTasks {
		m.AddJob()
	}

	wg.Wait()

}
