package model

// transfer2go data model implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/vkuznet/transfer2go/common"
	"github.com/vkuznet/transfer2go/utils"
)

// TransferCounter is a global atomic counter which keep tracks of transfers in the agent
var TransferCounter int32

// Processor is an object who process' given task
// The logic of the Processor should be implemented.
type Processor struct {
}

// Process defines execution process for a given task
func (e *Processor) Process(t *TransferRequest) error {
	return nil
}

// DefaultProcessor is a default processor instance
var DefaultProcessor = &Processor{}

// Request interface defines a task process
type Request interface {
	Process(*TransferRequest) error
}

// RequestFunc is a function type that implements the Request interface
type RequestFunc func(*TransferRequest) error

// Process is a method of TransferRequest
func (f RequestFunc) Process(t *TransferRequest) error {
	return f(t)
}

// Decorator wraps a request with extra behavior
type Decorator func(Request) Request

// helper function to perform transfer via HTTP protocol
func httpTransfer(rec CatalogEntry, t *TransferRequest) error {
	data, err := ioutil.ReadFile(rec.Lfn)
	if err != nil {
		return err
	}
	hash, b := utils.Hash(data)
	if hash != rec.Hash {
		return fmt.Errorf("File hash mismatch")
	}
	if b != rec.Bytes {
		return fmt.Errorf("File bytes mismatch")
	}
	url := fmt.Sprintf("%s/upload", t.DstUrl)
	td := TransferData{File: rec.Lfn, Dataset: rec.Dataset, Block: rec.Block, Data: data, Hash: hash, Bytes: b, SrcUrl: t.SrcUrl, SrcAlias: t.SrcAlias, DstUrl: t.DstUrl, DstAlias: t.DstAlias}
	d, e := json.Marshal(td)
	if e != nil {
		return e
	}
	resp := utils.FetchResponse(url, d)
	return resp.Error
}

// Transfer returns a Decorator that performs request transfers
func Transfer() Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			// increment number of transfers
			atomic.AddInt32(&TransferCounter, 1)

			// TODO: I need to decide how to deal with TransferCounter, so far:
			// decrement transfer counter when done with transfer request
			defer atomic.AddInt32(&TransferCounter, -1)

			log.Println("Request Transfer", t.String())

			records := TFC.Records(*t)
			if len(records) == 0 {
				// file does not exists in TFC, nothing to do, return immediately
				log.Printf("WARNING %v does match anything in TFC of this agent\n", t)
				return r.Process(t)
			}
			// obtain information about source and destination agents
			url := fmt.Sprintf("%s/status", t.DstUrl)
			resp := utils.FetchResponse(url, []byte{})
			if resp.Error != nil {
				return resp.Error
			}
			var dstAgent common.AgentStatus
			err := json.Unmarshal(resp.Data, &dstAgent)
			if err != nil {
				return err
			}
			url = fmt.Sprintf("%s/status", t.SrcUrl)
			resp = utils.FetchResponse(url, []byte{})
			if resp.Error != nil {
				return resp.Error
			}
			var srcAgent common.AgentStatus
			err = json.Unmarshal(resp.Data, &srcAgent)
			if err != nil {
				return err
			}

			// TODO: I need to implement bulk transfer for all files in found records
			// so far I loop over them individually and transfer one by one
			for _, rec := range records {

				// if protocol is not given use default one: HTTP
				var rpfn string // remote PFN
				if srcAgent.Protocol == "" || srcAgent.Protocol == "http" {
					log.Println("Transfer via HTTP protocol to", dstAgent)
					err = httpTransfer(rec, t)
					if err != nil {
						return err
					}
					rpfn = rec.Lfn
				} else {
					// construct remote PFN by using destination agent backend and record LFN
					rpfn = fmt.Sprintf("%s%s", dstAgent.Backend, rec.Lfn)
					// perform transfer with the help of backend tool
					cmd := exec.Command(srcAgent.Tool, srcAgent.ToolOpts, rec.Pfn, rpfn)
					if srcAgent.ToolOpts == "" {
						cmd = exec.Command(srcAgent.Tool, rec.Pfn, rpfn)
					}
					log.Println("Transfer command", cmd)
					err = cmd.Run()
					if err != nil {
						log.Println("ERROR", srcAgent.Tool, srcAgent.ToolOpts, rec.Pfn, rpfn, err)
						return err
					}
				}
				// Add entry for remote TFC after transfer is completed
				url = fmt.Sprintf("%s/tfc", t.DstUrl)
				r := CatalogEntry{Dataset: rec.Dataset, Block: rec.Block, Lfn: rec.Lfn, Pfn: rpfn, Bytes: rec.Bytes, Hash: rec.Hash}
				var records []CatalogEntry
				records = append(records, r)
				d, e := json.Marshal(records)
				if e != nil {
					return e
				}
				resp = utils.FetchResponse(url, d) // POST request
				if resp.Error != nil {
					return resp.Error
				}
			}

			return r.Process(t)
		})
	}
}

// Logging returns a Decorator that logs client requests
func Logging(l *log.Logger) Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			l.Println("TransferRequest", t)
			return r.Process(t)
		})
	}
}

// Pause returns a Decorator that pauses request for a given time interval
func Pause(interval time.Duration) Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			if interval > 0 {
				log.Println("TransferRequest", t, "is paused by", interval)
				time.Sleep(interval)
			}
			return r.Process(t)
		})
	}
}

// Tracer returns a Decorator that traces given request
func Tracer() Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			log.Println("Trace", t)
			return r.Process(t)
		})
	}
}

// Decorate decorates a Request r with all given Decorators
func Decorate(r Request, ds ...Decorator) Request {
	decorated := r
	for _, decorate := range ds {
		decorated = decorate(decorated)
	}
	return decorated
}
