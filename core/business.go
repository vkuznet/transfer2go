package core

// transfer2go core data transfer module
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rcrowley/go-metrics"
)

// Metrics of the agent
type Metrics struct {
	Transfers    metrics.Meter
	Meter        metrics.Meter
	WorkerMeters []metrics.Meter
}

// AgentMetrics defines various metrics about the agent work
var AgentMetrics Metrics

// TransferRequest data type
type TransferRequest struct {
	TimeStamp int64  `json:"ts"`       // timestamp of the request
	File      string `json:"file"`     // LFN name to be transferred
	Block     string `json:"block"`    // block name to be transferred
	Dataset   string `json:"dataset"`  // dataset name to be transferred
	SrcUrl    string `json:"srcUrl"`   // source agent URL which initiate the transfer
	SrcAlias  string `json:"srcAlias"` // source agent name
	DstUrl    string `json:"dstUrl"`   // destination agent URL which will consume the transfer
	DstAlias  string `json:"dstAlias"` // destination agent name
	Delay     int    `json:"delay"`    // transfer delay time, i.e. post-pone transfer
}

// String method return string representation of transfer request
func (t *TransferRequest) String() string {
	return fmt.Sprintf("<TransferRequest ts=%d file=%s block=%s dataset=%s srcUrl=%s srcAlias=%s dstUrl=%s dstAlias=%s delay=%d>", t.TimeStamp, t.File, t.Block, t.Dataset, t.SrcUrl, t.SrcAlias, t.DstUrl, t.DstAlias, t.Delay)
}

// Run method perform a job on transfer request
func (t *TransferRequest) Run() error {
	interval := time.Duration(t.Delay) * time.Second
	request := Decorate(DefaultProcessor,
		Pause(interval), // will pause a given request for a given interval
		Transfer(),
	)
	request.Process(t)
	return nil
}

// Job represents the job to be run
type Job struct {
	TransferRequest TransferRequest
}

// JobQueue is a buffered channel that we can send work requests on.
var JobQueue chan Job

// Worker represents the worker that executes the job
type Worker struct {
	Id         int
	JobPool    chan chan Job
	JobChannel chan Job
	quit       chan bool
}

// NewWorker return a new instance of the Worker type
func NewWorker(wid int, jobPool chan chan Job) Worker {
	return Worker{
		Id:         wid,
		JobPool:    jobPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.JobPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				AgentMetrics.WorkerMeters[w.Id].Mark(1)
				// we have received a work request.
				if err := job.TransferRequest.Run(); err != nil {
					log.Println("Error in job.TransferRequest.Run:", err.Error())
				}

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

// Dispatcher implementation
type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	JobPool    chan chan Job
	MaxWorkers int
}

// NewDispatcher returns new instance of Dispatcher type
func NewDispatcher(maxWorkers, maxQueue int, mfile string, minterval int64) *Dispatcher {
	// register metrics
	f, e := os.OpenFile(mfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		log.Fatalf("error opening file: %v", e)
	}
	defer f.Close()

	r := metrics.DefaultRegistry
	m := metrics.GetOrRegisterMeter("requests", r)
	go metrics.Log(r, time.Duration(minterval)*time.Second, log.New(f, "metrics: ", log.Lmicroseconds))

	// define agent metrics
	var workerMeters []metrics.Meter
	for i := 0; i < maxWorkers; i++ {
		wm := metrics.GetOrRegisterMeter(fmt.Sprintf("worker_%d", i), r)
		workerMeters = append(workerMeters, wm)
	}
	AgentMetrics = Metrics{Meter: m, WorkerMeters: workerMeters}

	// define pool of workers and jobqueue
	pool := make(chan chan Job, maxWorkers)
	JobQueue = make(chan Job, maxQueue)
	return &Dispatcher{JobPool: pool, MaxWorkers: maxWorkers}
}

// Run function starts the worker and dispatch it as go-routine
func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.JobPool)
		AgentMetrics.WorkerMeters[i].Mark(1)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue:
			AgentMetrics.Meter.Mark(1)
			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.JobPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		default:
			time.Sleep(time.Duration(10) * time.Millisecond) // wait for a job
		}
	}
}
