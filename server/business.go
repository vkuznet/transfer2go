// transfer2go agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package server

import (
	"encoding/json"
	//     "io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

// a random number generator
var RAND *rand.Rand

// TransferCollection holds data about transfer requests
type TransferCollection struct {
	Version  string            `json:"version"`
	Requests []TransferRequest `json:"data"`
	//     Requests []string `json:"data"`
}

// TransferRequest
type TransferRequest struct {
	File        string `json:"file"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Latency     int    `json:"latency"`
}

// Method to do a job on payload
func (t *TransferRequest) Run() error {
	interval := time.Duration(RAND.Int63n(10)) * time.Second
	// Usage example
	request := Decorate(DefaultProcessor,
		Pause(interval),
		Logging(log.New(os.Stdout, "request: ", log.LstdFlags)),
	)
	request.Process(t)
	return nil
}

// Job represents the job to be run
type Job struct {
	TransferRequest TransferRequest
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

// Worker represents the worker that executes the job
type Worker struct {
	Id         int
	JobPool    chan chan Job
	JobChannel chan Job
	quit       chan bool
}

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
				ServerMetrics.WorkerMeters[w.Id].Mark(1)
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

func NewDispatcher(maxWorkers, maxQueue int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	JobQueue = make(chan Job, maxQueue)
	RAND = rand.New(rand.NewSource(99))
	return &Dispatcher{JobPool: pool, MaxWorkers: maxWorkers}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.JobPool)
		ServerMetrics.WorkerMeters[i].Mark(0)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue:
			ServerMetrics.Meter.Mark(1)
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

// TransferRequest
func RequestHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Read the body into a string for json decoding
	var content = &TransferCollection{}
	log.Println("request", r)
	//     err := json.NewDecoder(io.LimitReader(r.Body, MaxLength)).Decode(&content)
	err := json.NewDecoder(r.Body).Decode(&content)
	if err != nil {
		log.Println("ERROR TransferCollection", err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Go through each payload and queue items individually to run job over the payload
	for _, rdoc := range content.Requests {

		// let's create a job with the payload
		work := Job{TransferRequest: rdoc}

		// Push the work onto the queue.
		JobQueue <- work
	}

	w.WriteHeader(http.StatusOK)
}
