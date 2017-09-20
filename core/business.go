package core

// transfer2go core data transfer module
// Author - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/rcrowley/go-metrics"
	logs "github.com/sirupsen/logrus"
	"github.com/vkuznet/transfer2go/utils"
)

// Metrics of the agent
type Metrics struct {
	In         metrics.Counter      // number of live transfer requests
	Failed     metrics.Counter      // number of failed transfer requests
	Total      metrics.Counter      // total number of transfer requests
	TotalBytes metrics.Counter      // total number of bytes by this agent
	Bytes      metrics.Counter      // number of bytes in progress
	CpuUsage   metrics.GaugeFloat64 // CPU usage in percentage
	MemUsage   metrics.GaugeFloat64 // Memory usage in MB
	Tick       metrics.Counter      // Store cpu ticks
	MaxTick    int64                // Max tick after which reset metrics
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
	Id        int64  `json:"id"`       // unique id of each request
	Priority  int    `json:"priority"` // priority of request
	Status    string `json:"status"`   // Identify the category of request
}

// Job represents the job to be run
type Job struct {
	TransferRequest TransferRequest `json:"request"` // TransferRequest
	Action          string          `json:"action"`  // Action to apply to TransferRequest, e.g. delete or transfer
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
	BufferSize int
}

// AgentMetrics defines various metrics about the agent work
var AgentMetrics Metrics

// StorageQueue is a buffered channel that we can send work requests on.
var StorageQueue chan Job

// RequestQueue is a queue to sort the requests according to priority.
var RequestQueue PriorityQueue

// TransferQueue is an instance of dispatcher to handle the transfer process
var TransferQueue chan Job

// TransferType decides which pull or push based model is used
var TransferType string

// MainAgent url
var MainAgent string

// Param to enable the router
var routerModel bool

// GetUsage method to get cpu and Memory usage
func (m *Metrics) GetUsage() (float64, float64, error) {
	cusage := AgentMetrics.CpuUsage.Value() / float64(AgentMetrics.Tick.Count())
	musage := AgentMetrics.MemUsage.Value() / float64(AgentMetrics.Tick.Count())
	if math.IsNaN(cusage) || math.IsNaN(musage) {
		return 0, 0, errors.New("Calculating system metrics")
	}
	return cusage, musage, nil
}

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
	return fmt.Sprintf("<TransferRequest id=%d priority=%d status=%s ts=%d file=%s block=%s dataset=%s srcUrl=%s srcAlias=%s dstUrl=%s dstAlias=%s delay=%d>", t.Id, t.Priority, t.Status, t.TimeStamp, t.File, t.Block, t.Dataset, t.SrcUrl, t.SrcAlias, t.DstUrl, t.DstAlias, t.Delay)
}

// RunPush method perform a job on transfer request. It will use push model
func (t *TransferRequest) RunPush() error {
	interval := time.Duration(t.Delay) * time.Second
	request := Decorate(DefaultProcessor,
		Pause(interval), // will pause a given request for a given interval
		PushTransfer(),
	)
	return request.Process(t)
}

// RunPull method perform a job on transfer request. It will use pull model
func (t *TransferRequest) RunPull() error {
	interval := time.Duration(t.Delay) * time.Second
	request := Decorate(DefaultProcessor,
		Pause(interval), // will pause a given request for a given interval
		PullTransfer(),
	)
	return request.Process(t)
}

// Delete performs deletion of transfer request
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

// String method return string representation of transfer request
func (j *Job) String() string {
	return fmt.Sprintf("<Job TransferRequest=%s action=%s>", j.TransferRequest.String(), j.Action)
}

// helper function to send request to main agent to update request status in its persistent store (REQUESTS table)
func (j *Job) UpdateRequest(status string) {
	furl := fmt.Sprintf("%s/action", MainAgent)
	var jobs []Job
	job := Job{TransferRequest: j.TransferRequest, Action: "update"}
	job.TransferRequest.Status = status
	jobs = append(jobs, job)
	data, err := json.Marshal(jobs)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
			"jobs":  jobs,
		}).Error("UpdateRequest unable to marshall jobs")
		return
	}
	resp := utils.FetchResponse(furl, data) // POST request
	// Check return status code
	if resp.StatusCode != 200 {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Error("UpdateRequest unable to send transfer request to main agent")
		return
	}
}

// RequestFails function to handle failed jobs
func (j *Job) RequestFails() {
	switch j.Action {
	case "store":
		// we stored request in a system, so do nothing here
	case "delete":
		// update main agent
		j.UpdateRequest("deleted")
	case "transfer":
		// update main agent
		j.UpdateRequest("error")
	}
}

// RequestSuccess function to handle success jobs
func (j *Job) RequestSuccess() {
	switch j.Action {
	case "store":
		// we stored request in a system, so do nothing here
	case "delete":
		// update main agent
		j.UpdateRequest("deleted")
	case "transfer":
		// update main agent
		j.UpdateRequest("finished")
	}
}

// NewWorker return a new instance of the Worker type
func NewWorker(wid int, bufferSize int, jobPool chan chan Job) Worker {
	return Worker{
		Id:         wid,
		JobPool:    jobPool,
		JobChannel: make(chan Job, bufferSize),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	var err error

	// register the current worker into the worker queue.
	w.JobPool <- w.JobChannel

	go func() {
		for {
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
					if TransferType == "push" {
						err = job.TransferRequest.RunPush()
					} else {
						err = job.TransferRequest.RunPull()
					}
				default:
					logs.WithFields(logs.Fields{
						"Action": job.Action,
					}).Error("Can't perform requested action")
				}

				if err != nil || job.TransferRequest.Status == "error" {
					// decide if we'll drop the request or put it on hold by increasing its delay
					// and put back to job channel
					if job.TransferRequest.Delay > 300 {
						logs.WithFields(logs.Fields{
							"Action":  job.Action,
							"Request": job.TransferRequest.String(),
						}).Error("Exceed number of iteration, discard request")
						job.RequestFails()
						AgentMetrics.Failed.Inc(1)
						w.JobPool <- w.JobChannel
					} else if job.TransferRequest.Delay > 0 {
						job.TransferRequest.Delay *= 2
						logs.WithFields(logs.Fields{
							"Error":   err.Error(),
							"Action":  job.Action,
							"Request": job.TransferRequest.String(),
						}).Warn("put on hold")
						w.JobChannel <- job
					} else {
						job.TransferRequest.Delay = 60
						logs.WithFields(logs.Fields{
							"Error":   err.Error(),
							"Action":  job.Action,
							"Request": job.TransferRequest.String(),
						}).Warn("put on hold")
						w.JobChannel <- job
					}
				} else if job.TransferRequest.Status != "" {
					// we got record which still in progress, e.g. agent stager is staging data
					// let's delay its processing and put it back to the job queue
					job.TransferRequest.Delay = 60
					logs.WithFields(logs.Fields{
						"Action":  job.Action,
						"Request": job.TransferRequest.String(),
					}).Warn("put on hold")
					w.JobChannel <- job
				} else {
					job.RequestSuccess()
					// decrement transfer counter
					AgentMetrics.In.Dec(1)
					w.JobPool <- w.JobChannel
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
func NewDispatcher(maxWorkers int, bufferSize int) *Dispatcher {
	// define pool of workers
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{JobPool: pool, MaxWorkers: maxWorkers, BufferSize: bufferSize}
}

// NewStager returns new instance of Dispatcher type
func NewStager(pool string, catalog Catalog) *FileSystemStager {
	if pool == "" {
		pool = "/tmp/transfer2go" // default pool area resides in /tmp
	}
	return &FileSystemStager{Pool: pool, Catalog: catalog}
}

// InitQueue initializes RequestQueue, transferQueue and StorageQueue
func InitQueue(transferQueueSize int, storageQueueSize int, mfile string, minterval int64, monitorTime int64, router bool) {
	// register metrics
	f, e := os.OpenFile(mfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		logs.WithFields(logs.Fields{
			"Error": e,
		}).Error("Error opening file:")
	}

	// define agent's metrics
	r := metrics.DefaultRegistry
	inT := metrics.GetOrRegisterCounter("inTransfer", r)
	failT := metrics.GetOrRegisterCounter("failedTransfers", r)
	totT := metrics.GetOrRegisterCounter("totalTransfers", r)
	totB := metrics.GetOrRegisterCounter("totalBytes", r)
	bytesT := metrics.GetOrRegisterCounter("bytesInTransfer", r)
	cpuUsage := metrics.GetOrRegisterGaugeFloat64("cpuUsage", r)
	tick := metrics.GetOrRegisterCounter("tick", r)
	memUsage := metrics.GetOrRegisterGaugeFloat64("memUsage", r)
	timeTick := monitorTime / minterval
	AgentMetrics = Metrics{In: inT, Failed: failT, Total: totT, TotalBytes: totB, Bytes: bytesT, CpuUsage: cpuUsage, MemUsage: memUsage, Tick: tick, MaxTick: timeTick}

	// Calculate the machine usage for the first time
	AgentMetrics.GetCurrentStats()

	// Run background process to calculate machine usage
	go func() {
		for range time.Tick(time.Duration(minterval) * time.Second) {
			AgentMetrics.GetCurrentStats()
		}
	}()

	go func() {
		defer f.Close()
		metrics.Log(r, time.Duration(minterval)*time.Second, log.New(f, "metrics: ", log.Lmicroseconds))
	}()

	// initialize Storage and Request queues
	StorageQueue = make(chan Job, storageQueueSize)
	RequestQueue = make(PriorityQueue, 0) // Create a priority queue

	// Load pending requests from DB
	heap.Init(&RequestQueue)
	requests, err := TFC.ListRequest("pending") // Load requests from persistent store (REQUESTS table)
	check("Unable To fetch data", err)
	for i := 0; i < len(requests); i++ {
		heap.Push(&RequestQueue, &Item{Value: requests[i], priority: requests[i].Priority})
	}
	if router == true {
		routerModel = router
		AgentRouter.InitialTrain()
	}
	logs.Println("Requests restored from db")
	TransferQueue = make(chan Job, transferQueueSize)
}

// StorageRunner function starts the worker and dispatch it as go-routine
func (d *Dispatcher) StorageRunner() {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.BufferSize, d.JobPool)
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

// TransferRunner function starts the worker and dispatch it as go-routine
func (d *Dispatcher) TransferRunner() {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(i, d.BufferSize, d.JobPool)
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

// GetCurrentStats function to get current system usage
func (m *Metrics) GetCurrentStats() {
	cused, err1 := utils.UsedCPU()
	mused, err2 := utils.UsedRAM()
	if err1 == nil && err2 == nil {
		if AgentMetrics.Tick.Count() > m.MaxTick {
			AgentMetrics.Tick.Clear()
			AgentMetrics.CpuUsage.Update(0)
			AgentMetrics.MemUsage.Update(0)
		}
		AgentMetrics.Tick.Inc(1)
		AgentMetrics.CpuUsage.Update(AgentMetrics.CpuUsage.Value() + cused)
		AgentMetrics.MemUsage.Update(AgentMetrics.MemUsage.Value() + mused)
	}
}
