package model

// transfer2go data model implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sync/atomic"
	"time"

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
			// TODO: main Transfer logic would be implemented here
			// so far we call simple log.Println and later we'll transfer the request here
			log.Println("Transfer", t) // REPLACE WITH ACTUAL CODE

			fname, fhash, fbytes := TFC.FileInfo(t.File)
			if TFC.Type == "filesystem" {
				data, err := ioutil.ReadFile(fname)
				if err != nil {
					return err
				}
				hash, b := Hash(data)
				if hash != fhash {
					return fmt.Errorf("File hash mismatch")
				}
				if b != fbytes {
					return fmt.Errorf("File bytes mismatch")
				}
				url := fmt.Sprintf("%s/transfer", t.DstUrl)
				td := TransferData{File: fname, Data: data, Hash: hash, Bytes: b, SrcUrl: t.SrcUrl, SrcAlias: t.SrcAlias, DstUrl: t.DstUrl, DstAlias: t.DstAlias}
				d, e := json.Marshal(td)
				if e != nil {
					return e
				}
				resp := utils.FetchResponse(url, d)
				return resp.Error
			} else if TFC.Type == "sqlitedb" {
				log.Println("Not Implemented Yet")
			}

			// if transfer is successful decrement transfer counter
			atomic.AddInt32(&TransferCounter, -1)

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
