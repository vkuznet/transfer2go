package core

// transfer2go core data transfer module
// Author - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"container/heap"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
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

// Job represents the job to be run
type Job struct {
	TransferRequest TransferRequest
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

// JobQueue is a buffered channel that we can send work requests on.
var JobQueue chan Job

var RequestQueue PriorityQueue

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
				// Add info to agents metrics
				AgentMetrics.In.Inc(1)
				// we have received a work request.
				if err := job.TransferRequest.Run(); err != nil {
					msg := fmt.Sprintf("WARNING %v experienced an error %v, put on hold", job.TransferRequest, err.Error())
					// decide if we'll drop the request or put it on hold by increasing its delay and put back to job channel
					if job.TransferRequest.Delay > 300 {
						logs.WithFields(logs.Fields{
							"Transfer Request": job.TransferRequest,
						}).Error("Exceed number of iteration, discard request")
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
func NewDispatcher(maxWorkers, maxQueue int, mfile string, minterval int64) *Dispatcher {
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

	// define pool of workers and jobqueue
	pool := make(chan chan Job, maxWorkers)
	JobQueue = make(chan Job, maxQueue)
	return &Dispatcher{JobPool: pool, MaxWorkers: maxWorkers}
}

// initialize heap from db
func InitHeap() PriorityQueue {
	// Create a priority queue
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	// Load from database
	stm := getSQL("list_request")
	rows, err := DB.Query(stm, "pending")
	check("Unable to perform DB.Query over Request table", err)
	cols, err := rows.Columns()
	check("Unable to perform DB.Query over Request table", err)
	pointers := make([]interface{}, len(cols))
	container := make([]string, len(cols)) // A pointer to Columns of db
	for i, _ := range pointers {
		pointers[i] = &container[i]
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(pointers...)
		id, err := strconv.ParseInt(container[0], 10, 64)
		check("Unable to read id", err)
		priority, err := strconv.Atoi(container[7])
		check("Unable to read priority", err)
		item := &Item{
			Value:    TransferRequest{SrcUrl: container[4], DstUrl: container[5], File: container[1], Block: container[2], Dataset: container[3]},
			priority: priority,
			Id:       id,
		}
		heap.Push(&RequestQueue, item)
	}
	log.Println("Requests restored from db")
	return pq
}

// Run function starts the worker and dispatch it as go-routine
func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.JobPool)
		worker.Start()
	}

	RequestQueue = InitHeap()
	go d.store()
}

// Whenever new job comes in put it in heap. And also store it in sqlite db.
func (d *Dispatcher) store() {
	for {
		select {
		case job := <-JobQueue:
			item := &Item{
				Value:    job.TransferRequest,
				priority: 1,
				Id:       time.Now().Unix(),
			}
			heap.Push(&RequestQueue, item)
			stm := getSQL("insert_request")
			_, err := DB.Exec(stm, item.Id, job.TransferRequest.File, job.TransferRequest.Block, job.TransferRequest.Dataset, job.TransferRequest.SrcUrl, job.TransferRequest.DstUrl, "pending", 1)
			if err != nil {
				if !strings.Contains(err.Error(), "UNIQUE") {
					check("Unable to insert into blocks table", err)
				}
			} else {
				logs.WithFields(logs.Fields{
					"Request": job.TransferRequest,
				}).Println("Request Registered")
			}
		default:
			time.Sleep(time.Duration(10) * time.Millisecond)
		}
	}
}

func (d *Dispatcher) dispatch(queue PriorityQueue) {
	for {
		select {
		case job := <-JobQueue:
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
