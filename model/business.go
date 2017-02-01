package model

// transfer2go data model module
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/vkuznet/transfer2go/utils"

	// loads sqlite3 database layer
	_ "github.com/mattn/go-sqlite3"
)

// global pointer to DB
var _db *sql.DB

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// CatalogEntry represents an entry in TFC
type CatalogEntry struct {
	Lfn     string `json:"lfn"`     // lfn stands for Logical File Name
	Pfn     string `json:"pfn"`     // pfn stands for Physical File Name
	Dataset string `json:"dataset"` // dataset represents collection of blocks
	Block   string `json:"block"`   // block idetify single block withing a dataset
	Bytes   int64  `json:"bytes"`   // size of the files in bytes
	Hash    string `json:"hash"`    // hash represents checksum of the pfn
}

// Catalog represents Trivial File Catalog (TFC) of the model
type Catalog struct {
	Type     string `json:"type"`     // catalog type, e.g. filesystem, sqlitedb, etc.
	Uri      string `json:"uri"`      // catalog uri, e.g. sqlitedb:///file.db
	Login    string `json:"login"`    // database login
	Password string `json:"password"` // database password
	Owner    string `json:"owner"`    // used by ORACLE DB, defines owner of the database
}

func (c *Catalog) Open() {
	if _db == nil {
		dbtype := c.Type
		dburi := c.Uri // TODO: I may need to change this for MySQL/ORACLE
		_db, dberr := sql.Open(dbtype, dburi)
		defer _db.Close()
		if dberr != nil {
			log.Fatal(dberr)
		}
		dberr = _db.Ping()
		if dberr != nil {
			log.Fatal(dberr)
		}
		_db.SetMaxOpenConns(100)
		_db.SetMaxIdleConns(100)
	}
}

// Find method look-up entries in a catalog for a given query
func (c *Catalog) Find(stm string, cols []string, vals []interface{}, args ...interface{}) []CatalogEntry {
	var out []CatalogEntry
	rows, err := _db.Query(stm, args...)
	if err != nil {
		msg := fmt.Sprintf("ERROR _db.Query, query='%s' args='%v' error=%v", stm, args, err)
		log.Fatal(msg)
	}
	defer rows.Close()

	// loop over rows
	for rows.Next() {
		err := rows.Scan(vals...)
		if err != nil {
			msg := fmt.Sprintf("ERROR rows.Scan, vals='%v', error=%v", vals, err)
			log.Fatal(msg)
		}
		rec := CatalogEntry{}
		rec.Dataset = vals[0].(string)
		rec.Block = vals[1].(string)
		rec.Lfn = vals[2].(string)
		rec.Pfn = vals[3].(string)
		rec.Bytes = vals[4].(int64)
		rec.Hash = vals[5].(string)
		//         for i, _ := range cols {
		//             rec[cols[i]] = vals[i]
		//         }
		out = append(out, rec)
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
	return out
}

// Add method adds entry to a catalog
func (c *Catalog) Add(entry CatalogEntry) error {
	// open DB connection
	c.Open()

	// add entry to the catalog
	tx, e := _db.Begin()
	check(e)

	var stm string
	// get dataset id
	stm = fmt.Sprintf("SELECT id FROM DATASETS WHERE dataset=?")
	did, e1 := _db.Exec(stm, entry.Dataset)
	check(e1)

	// insert dataset into dataset tables
	stm = fmt.Sprintf("INSERT INTO DATASETS(dataset) VALUES(?)")
	_, e2 := _db.Exec(stm, entry.Dataset)
	check(e2)

	// get block id
	stm = fmt.Sprintf("SELECT id FROM BLOCKS WHERE block=?")
	bid, e3 := _db.Exec(stm, entry.Block)
	check(e3)

	// insert block into block table
	stm = fmt.Sprintf("INSERT INTO BLOCKS(block) VALUES(?)")
	_, e4 := _db.Exec(stm, entry.Block)
	check(e4)

	// insert entry into files table
	stm = fmt.Sprintf("INSERT INTO FILES(lfn, pfn, blockid, datasetid, bytes, hash) VALUES(?,?,?,?,?,?)")
	_, e5 := _db.Exec(stm, entry.Lfn, entry.Pfn, bid, did, entry.Bytes, entry.Hash)
	check(e5)

	tx.Commit()

	return nil
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
					files = append(files, fmt.Sprintf("%s/%s", c.Uri, f.Name()))
				}
			} else {
				files = append(files, fmt.Sprintf("%s/%s", c.Uri, f.Name()))
			}
		}
		return files
	} else if c.Type == "sqlitedb" {
		// construct SQL query
		var args []interface{} // argument values passed to SQL statment
		cols := []string{"dataset", "blockid", "lfn", "pfn", "bytes", "hash"}
		stm := fmt.Sprintf("SELECT %s FROM FILES AS F JOIN BLOCKS AS B ON F.BLOCKID=B.ID JOIN DATASETS AS D ON F.DATASETID = D.ID", strings.Join(cols, ","))
		vals := []interface{}{new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullString), new(sql.NullInt64), new(sql.NullString)}
		for _, entry := range c.Find(stm, cols, vals, args) {
			files = append(files, entry.Lfn)
		}
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
		hash, b := utils.Hash(data)
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
