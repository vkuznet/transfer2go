package model

// transfer2go data model module
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
	Meter        metrics.Meter
	WorkerMeters []metrics.Meter
}

// AgentMetrics defines various metrics about the agent work
var AgentMetrics Metrics

// TransferCollection holds data about transfer requests
type TransferCollection struct {
	TimeStamp int64             `json:"ts"`
	Requests  []TransferRequest `json:"data"`
}

// TransferRequest data type
type TransferRequest struct {
	TimeStamp int64  `json:"ts"`
	File      string `json:"file"`
	SrcUrl    string `json:"srcUrl"`
	SrcAlias  string `json:"srcAlias"`
	DstUrl    string `json:"dstUrl"`
	DstAlias  string `json:"dstAlias"`
	Latency   int    `json:"latency"`
}

// String method return string representation of transfer request
func (t *TransferRequest) String() string {
	return fmt.Sprintf("<TransferRequest ts=%d, file=%s, srcUrl=%s srcAlias=%s dstUrl=%s dstAlias=%s latency=%d>", t.TimeStamp, t.File, t.SrcUrl, t.SrcAlias, t.DstUrl, t.DstAlias, t.Latency)
}

// Run method perform a job on transfer request
func (t *TransferRequest) Run() error {
	interval := time.Duration(t.Latency) * time.Second
	request := Decorate(DefaultProcessor,
		Pause(interval), // will pause a given request for a given interval
		Transfer(),
	)
	request.Process(t)
	return nil
}

// TransferData extends TransferRequest interface to include transferring data, checksum, bytes
type TransferData struct {
	File     string `json:"file"`
	Block    string `json:"block"`
	Dataset  string `json:"dataset"`
	SrcUrl   string `json:"srcUrl"`
	SrcAlias string `json:"srcAlias"`
	DstUrl   string `json:"dstUrl"`
	DstAlias string `json:"dstAlias"`
	Data     []byte `json:"data"`
	Hash     string `json:"hash"`
	Bytes    int64  `json:"bytes"`
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
		AgentMetrics.WorkerMeters[i].Mark(0)
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
