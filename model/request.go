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

// Transfer returns a Decorator that performs request transfers
func Transfer() Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			// increment number of transfers
			atomic.AddInt32(&TransferCounter, 1)

			// TODO: I need to decide how to deal with TransferCounter, so far:
			// decrement transfer counter when done with transfer request
			defer atomic.AddInt32(&TransferCounter, -1)

			// TODO: main Transfer logic would be implemented here
			log.Println("Request Transfer", t)

			rec := TFC.FileInfo(t.File)
			if rec.Lfn == "" {
				// file does not exists in TFC, nothing to do, return immediately
				log.Printf("WARNING requested file %s does not exists in TFC of this agent\n", t.File)
				return r.Process(t)
			}
			if TFC.Type == "filesystem" {
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
				url := fmt.Sprintf("%s/transfer", t.DstUrl)
				td := TransferData{File: rec.Lfn, Dataset: rec.Dataset, Block: rec.Block, Data: data, Hash: hash, Bytes: b, SrcUrl: t.SrcUrl, SrcAlias: t.SrcAlias, DstUrl: t.DstUrl, DstAlias: t.DstAlias}
				d, e := json.Marshal(td)
				if e != nil {
					return e
				}
				resp := utils.FetchResponse(url, d)
				return resp.Error
			} else if TFC.Type == "sqlite3" {
				// TODO: This should be a go-routine
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
				pfn := fmt.Sprintf("%s%s", srcAgent.Backend, rec.Lfn)
				rpfn := fmt.Sprintf("%s%s", dstAgent.Backend, rec.Lfn)
				// TODO: I'm not sure if I need a record in local TFC since I do transfer from local agent which knows about the file
				// Add transfer entry in local TFC
				//                 entry := CatalogEntry{Dataset: rec.Dataset, Block: rec.Block, Lfn: rec.Lfn, Pfn: rec.Pfn, Bytes: rec.Bytes, Hash: rec.Hash}
				//                 TFC.Add(entry)
				// perform transfer with the help of backend tool
				cmd := exec.Command(srcAgent.Tool, pfn, rpfn)
				log.Println("Transfer command", cmd)
				err = cmd.Run()
				if err != nil {
					log.Println("ERROR", srcAgent.Tool, pfn, rpfn)
					return err
				}
				// Add entry for remote TFC after transfer is completed
				url = fmt.Sprintf("%s/tfc", t.DstUrl)
				rEntry := CatalogEntry{Dataset: rec.Dataset, Block: rec.Block, Lfn: rec.Lfn, Pfn: rpfn, Bytes: rec.Bytes, Hash: rec.Hash}
				data, err := json.Marshal(rEntry)
				if err != nil {
					return err
				}
				resp = utils.FetchResponse(url, data) // POST request
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
