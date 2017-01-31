package model

// transfer2go data model module
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/rcrowley/go-metrics"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	// loads sqlite3 database layer
	_ "github.com/mattn/go-sqlite3"
)

// Catalog represents Trivial File Catalog of the model
type Catalog struct {
	Type     string `json:"type"`
	Uri      string `json:"uri"`
	Login    string `json:"login"`
	Password string `json:"password"`
}

func filePath(idir, fname string) string {
	return fmt.Sprintf("%s/%s", idir, fname)
}

// Files method of catalog returns list of files known in catalog
// TODO: implement sqlitedb catalog logic, e.g. we need to make
// a transfer and then record in DB catalog file's hash and transfer details
func (c *Catalog) Files(pattern string) []string {
	var files []string
	if c.Type == "filesystem" {
		filesInfo, err := ioutil.ReadDir(c.Uri)
		if err != nil {
			log.Println("ERROR: unable to list files in catalog", c.Uri, err)
			return []string{}
		}
		for _, f := range filesInfo {
			if pattern != "" {
				if strings.Contains(f.Name(), pattern) {
					files = append(files, filePath(c.Uri, f.Name()))
				}
			} else {
				files = append(files, filePath(c.Uri, f.Name()))
			}
		}
		return files
	} else if c.Type == "sqlitedb" {
		db, err := sql.Open(c.Type, c.Uri)
		defer db.Close()
		if err != nil {
			log.Println("ERROR: unable to list files in catalog", c.Uri, err)
			return []string{}
		}
		db.SetMaxOpenConns(100)
		db.SetMaxIdleConns(100)
		return files
	}
	return files
}

// FileInfo provides information about given file name in Catalog
func (c *Catalog) FileInfo(fileEntry string) (string, string, int64) {
	if c.Type == "filesystem" {
		fname := fileEntry
		data, err := ioutil.ReadFile(fname)
		if err != nil {
			log.Println("ERROR, unable to read a file", fname, err)
		}
		hash, b := Hash(data)
		return fname, hash, b
	} else if c.Type == "sqlitedb" {
		log.Println("Not Implemented Yet")
	}
	return fileEntry, "", 0
}

// TFC stands for Trivial File Catalog
var TFC Catalog

// Metrics of the agent
type Metrics struct {
	Meter        metrics.Meter
	WorkerMeters []metrics.Meter
}

// AgentMetrics defines various metrics about the agent work
var AgentMetrics Metrics

// Hash implements hash function for data, it returns a hash and number of bytes
func Hash(data []byte) (string, int64) {
	hasher := sha256.New()
	b, e := hasher.Write(data)
	if e != nil {
		log.Println("ERROR, Unable to write chunk of data via hasher.Write", e)
	}
	return hex.EncodeToString(hasher.Sum(nil)), int64(b)
}

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
