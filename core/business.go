package core

// transfer2go core data transfer module
// Author - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"container/heap"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rcrowley/go-metrics"
	logs "github.com/sirupsen/logrus"
)

// Metrics of the agent
type Metrics struct {
	In         metrics.Counter // number of live transfer requests
	Failed     metrics.Counter // number of failed transfer requests
	Total      metrics.Counter // total number of transfer requests
	TotalBytes metrics.Counter // total number of bytes by this agent
	Bytes      metrics.Counter // number of bytes in progress
}

// TransferRequest data type
type TransferRequest struct {
	TimeStamp     int64          `json:"ts"`       // timestamp of the request
	File          string         `json:"file"`     // LFN name to be transferred
	Block         string         `json:"block"`    // block name to be transferred
	Dataset       string         `json:"dataset"`  // dataset name to be transferred
	SrcUrl        string         `json:"srcUrl"`   // source agent URL which initiate the transfer
	SrcAlias      string         `json:"srcAlias"` // source agent name
	DstUrl        string         `json:"dstUrl"`   // destination agent URL which will consume the transfer
	DstAlias      string         `json:"dstAlias"` // destination agent name
	Delay         int            `json:"delay"`    // transfer delay time, i.e. post-pone transfer
	Id            int64          `json:"id"`       // unique id of each request
	Priority      int            `json:"priority"` // priority of request
	Status        string         `json:"status"`   // Identify the category of request
	FailedRecords []CatalogEntry // Store records which are failed
}

// Job represents the job to be run
type Job struct {
	TransferRequest TransferRequest `json:"request"`
	Action          string          `json:"action"`
}

// Worker represents the worker that executes the job
type Worker struct {
	Id         int
	JobPool    chan chan Job
	JobChannel chan Job
	quit       chan bool
}

// Dispatcher implementation
type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	JobPool    chan chan Job
	MaxWorkers int
}

// AgentMetrics defines various metrics about the agent work
var AgentMetrics Metrics

// StorageQueue is a buffered channel that we can send work requests on.
var StorageQueue chan Job

// A queue to sort the requests according to priority.
var RequestQueue PriorityQueue

// An instance of dispatcher to handle the transfer process
var TransferQueue chan Job

// String representation of Metrics
func (m *Metrics) String() string {
	return fmt.Sprintf("<Metrics: in=%d failed=%d total=%d bytes=%d totBytes=%d>", m.In.Count(), m.Failed.Count(), m.Total.Count(), m.Bytes.Count(), m.TotalBytes.Count())
}

// ToDict converts Metrics structure to a map
func (m *Metrics) ToDict() map[string]int64 {
	dict := make(map[string]int64)
	dict["in"] = m.In.Count()
	dict["failed"] = m.Failed.Count()
	dict["total"] = m.Total.Count()
	dict["totalBytes"] = m.TotalBytes.Count()
	dict["bytes"] = m.Bytes.Count()
	return dict
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
	return request.Process(t)
}

func (t *TransferRequest) Delete() error {
	interval := time.Duration(t.Delay) * time.Second
	request := Decorate(DefaultProcessor,
		Pause(interval), // will pause a given request for a given interval
		Delete(),
	)
	return request.Process(t)
}

// Store method stores a job in heap and db
func (t *TransferRequest) Store() error {
	interval := time.Duration(t.Delay) * time.Second
	request := Decorate(DefaultProcessor,
		Pause(interval), // will pause a given request for a given interval
		Store(),
	)
	return request.Process(t)
}

// Function to handle failed jobs
func (j *Job) RequestFails() {
	switch j.Action {
	case "store":
		// TODO: notify client about error
	case "delete":
		// TODO: notify site manager about error. Also do not change the status in DB.
		err := TFC.UpdateRequest(j.TransferRequest.Id, "pending")
		if err != nil {

		}
	case "transfer":
		err := TFC.UpdateRequest(j.TransferRequest.Id, "error")
		if err == nil {
			// Transfer process fails and successfully updated status in DB.
			index := -1
			for _, item := range RequestQueue {
				if item.Value.Id == j.TransferRequest.Id {
					index = item.index
					break
				}
			}
			if index < RequestQueue.Len() && index >= 0 {
				heap.Remove(&RequestQueue, index)
			} else {
				// Transfer process fails but can't find it in heap.
			}
		} else {
			// Transfer process fails and cant updated status in DB.
		}
	}
}

// Function to handle success jobs
func (j *Job) RequestSuccess() {
	switch j.Action {
	case "store":
	case "delete":
	case "transfer":
		err := TFC.UpdateRequest(j.TransferRequest.Id, "finished")
		if err == nil {
			// Transfer process passed and successfully updated status in DB.
			index := -1
			for _, item := range RequestQueue {
				if item.Value.Id == j.TransferRequest.Id {
					index = item.index
					break
				}
			}
			if index < RequestQueue.Len() && index >= 0 {
				heap.Remove(&RequestQueue, index)
			} else {
				// Transfer process passed but can't find it in heap.
			}
		} else {
			// Transfer process passed and cant updated status in DB.
		}
	}
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
	var err error
	go func() {
		for {
			// register the current worker into the worker queue.
			w.JobPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// Add info to agents metrics
				AgentMetrics.In.Inc(1)
				// we have received a work request.
				switch job.Action {
				case "store":
					err = job.TransferRequest.Store()
				case "delete":
					err = job.TransferRequest.Delete()
				case "transfer":
					err = job.TransferRequest.Run()
				default:
					logs.WithFields(logs.Fields{
						"Action": job.Action,
					}).Error("Can't perform requested action")
				}

				if err != nil || job.TransferRequest.Status == "error" {
					msg := fmt.Sprintf("WARNING %v experienced an error %v, %v, put on hold", job.TransferRequest, err.Error(), job.TransferRequest.Status)
					// decide if we'll drop the request or put it on hold by increasing its delay and put back to job channel
					if job.TransferRequest.Delay > 300 {
						logs.WithFields(logs.Fields{
							"Transfer Request": job.TransferRequest,
						}).Error("Exceed number of iteration, discard request")
						job.RequestFails()
						AgentMetrics.Failed.Inc(1)
					} else if job.TransferRequest.Delay > 0 {
						job.TransferRequest.Delay *= 2
						logs.Println(msg)
						w.JobChannel <- job
					} else {
						job.TransferRequest.Delay = 60
						logs.Println(msg)
						w.JobChannel <- job
					}
				} else {
					job.RequestSuccess()
					// decrement transfer counter
					AgentMetrics.In.Dec(1)
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

// NewDispatcher returns new instance of Dispatcher type
func NewDispatcher(maxWorkers int) *Dispatcher {
	// define pool of workers
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{JobPool: pool, MaxWorkers: maxWorkers}
}

// initialize RequestQueue, transferQueue and StorageQueue
func InitQueue(transferQueueSize int, storageQueueSize int, mfile string, minterval int64) {
	// register metrics
	f, e := os.OpenFile(mfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		logs.WithFields(logs.Fields{
			"Error": e,
		}).Error("Error opening file:")
	}
	defer f.Close()

	// define agent's metrics
	r := metrics.DefaultRegistry
	inT := metrics.GetOrRegisterCounter("inTransfer", r)
	failT := metrics.GetOrRegisterCounter("failedTransfers", r)
	totT := metrics.GetOrRegisterCounter("totalTransfers", r)
	totB := metrics.GetOrRegisterCounter("totalBytes", r)
	bytesT := metrics.GetOrRegisterCounter("bytesInTransfer", r)
	AgentMetrics = Metrics{In: inT, Failed: failT, Total: totT, TotalBytes: totB, Bytes: bytesT}
	go metrics.Log(r, time.Duration(minterval)*time.Second, log.New(f, "metrics: ", log.Lmicroseconds))

	StorageQueue = make(chan Job, storageQueueSize)
	TransferQueue = make(chan Job, transferQueueSize)
	RequestQueue = make(PriorityQueue, 0) // Create a priority queue

	// Load pending requests from DB
	heap.Init(&RequestQueue)
	requests, err := TFC.ListRequest("pending") // Load requests from database
	check("Unable To fetch data", err)
	for i := 0; i < len(requests); i++ {
		heap.Push(&RequestQueue, &Item{Value: requests[i], priority: requests[i].Priority})
	}
	logs.Println("Requests restored from db")
}

// Run function starts the worker and dispatch it as go-routine
func (d *Dispatcher) StorageRunner() {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.JobPool)
		worker.Start()
	}

	go d.dispatchToStorage()
}

func (d *Dispatcher) dispatchToStorage() {
	for {
		select {
		case job := <-StorageQueue:
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

// Run function starts the worker and dispatch it as go-routine
func (d *Dispatcher) TransferRunner() {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.JobPool)
		worker.Start()
	}

	go d.dispatchToTransfer()
}

func (d *Dispatcher) dispatchToTransfer() {
	for {
		select {
		case job := <-TransferQueue:
			// a job request has been received
			go func(job Job) {
				// Update the status of request in DB
				err, status := TFC.GetStatus(job.TransferRequest.Id)
				if status == "pending" {
					err = TFC.UpdateRequest(job.TransferRequest.Id, "processing")
				} else {
					return
				}
				err = TFC.RetriveRequest(&job.TransferRequest)
				if err != nil {
					// TODO: push in error queue.
					return
				}
				err = TFC.UpdateRequest(job.TransferRequest.Id, "processing")
				if err != nil {
					// TODO: push in error queue.
					return
				}
				job.TransferRequest.Status = "processing"
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
