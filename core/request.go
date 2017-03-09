package core

// transfer2go data core module, request implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/vkuznet/transfer2go/utils"
)

// AgentStatus data type
type AgentStatus struct {
	Url       string            `json:"url"`      // agent url
	Name      string            `json:"name"`     // agent name or alias
	TimeStamp int64             `json:"ts"`       // time stamp
	Catalog   string            `json:"catalog"`  // underlying TFC catalog
	Protocol  string            `json:"protocol"` // underlying transfer protocol
	Backend   string            `json:"backend"`  // underlying transfer backend
	Tool      string            `json:"tool"`     // underlying transfer tool, e.g. xrdcp
	ToolOpts  string            `json:"toolopts"` // options for backend tool
	Agents    map[string]string `json:"agents"`   // list of known agents
	Addrs     []string          `json:"addrs"`    // list of all IP addresses
	Metrics   map[string]int64  `json:"metrics"`  // agent metrics
}

// Processor is an object who process' given task
// The logic of the Processor should be implemented.
type Processor struct {
}


// Request interface defines a task process
type Request interface {
	Process(*TransferRequest) error
}

// RequestFunc is a function type that implements the Request interface
type RequestFunc func(*TransferRequest) error

// Decorator wraps a request with extra behavior
type Decorator func(Request) Request


// DefaultProcessor is a default processor instance
var DefaultProcessor = &Processor{}

// String provides string representation of given agent status
func (a *AgentStatus) String() string {
	return fmt.Sprintf("<Agent name=%s url=%s catalog=%s protocol=%s backend=%s tool=%s toolOpts=%s agents=%v addrs=%v metrics(%v)>", a.Name, a.Url, a.Catalog, a.Protocol, a.Backend, a.Tool, a.ToolOpts, a.Agents, a.Addrs, a.Metrics)
}

// Process defines execution process for a given task
func (e *Processor) Process(t *TransferRequest) error {
	return nil
}

// Process is a method of TransferRequest
func (f RequestFunc) Process(t *TransferRequest) error {
	return f(t)
}

// filleTransferRequest creates HTTP request to transfer a given file name
// https://matt.aimonetti.net/posts/2013/07/01/golang-multipart-file-upload-example/
func fileTransferRequest(c CatalogEntry, tr *TransferRequest) (*http.Request, error) {
	file, err := os.Open(c.Pfn)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("data", filepath.Base(c.Pfn))
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(part, file)
	err = writer.Close()
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/upload", tr.DstUrl)
	req, err := http.NewRequest("POST", url, body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Pfn", c.Pfn)
	req.Header.Set("Lfn", c.Lfn)
	req.Header.Set("Bytes", fmt.Sprintf("%d", c.Bytes))
	req.Header.Set("Hash", c.Hash)
	req.Header.Set("Src", tr.SrcAlias)
	req.Header.Set("Dst", tr.DstAlias)
	return req, err
}

// helper function to perform transfer via HTTP protocol
func httpTransfer(c CatalogEntry, t *TransferRequest) (string, error) {
	// create file transfer request
	request, err := fileTransferRequest(c, t)
	if err != nil {
		return "", err
	}
	client := utils.HttpClient()
	resp, err := client.Do(request)
	defer resp.Body.Close()

	var r CatalogEntry
	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return "", err
	}
	return r.Pfn, nil
}

// Transfer returns a Decorator that performs request transfers
func Transfer() Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
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
			var dstAgent AgentStatus
			err := json.Unmarshal(resp.Data, &dstAgent)
			if err != nil {
				return err
			}
			url = fmt.Sprintf("%s/status", t.SrcUrl)
			resp = utils.FetchResponse(url, []byte{})
			if resp.Error != nil {
				return resp.Error
			}
			var srcAgent AgentStatus
			err = json.Unmarshal(resp.Data, &srcAgent)
			if err != nil {
				return err
			}

			// TODO: I need to implement bulk transfer for all files in found records
			// so far I loop over them individually and transfer one by one
			var trRecords []CatalogEntry // list of successfully transferred records
			for _, rec := range records {

				time0 := time.Now().Unix()

				AgentMetrics.Bytes.Inc(rec.Bytes)

				// if protocol is not given use default one: HTTP
				var rpfn string // remote PFN
				if srcAgent.Protocol == "" || srcAgent.Protocol == "http" {
					log.Println("Transfer via HTTP protocol to", dstAgent.String())
					rpfn, err = httpTransfer(rec, t)
					if err != nil {
						log.Println("ERROR Transfer", rec.String(), t.String(), err)
						continue // if we fail on single record we continue with others
					}
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
						log.Println("ERROR Transfer", srcAgent.Tool, srcAgent.ToolOpts, rec.Pfn, rpfn, err)
						continue // if we fail on single record we continue with others
					}
				}
				r := CatalogEntry{Dataset: rec.Dataset, Block: rec.Block, Lfn: rec.Lfn, Pfn: rpfn, Bytes: rec.Bytes, Hash: rec.Hash, TransferTime: (time.Now().Unix() - time0), Timestamp: time.Now().Unix()}
				trRecords = append(trRecords, r)

				// record how much we transferred
				AgentMetrics.TotalBytes.Inc(r.Bytes) // keep growing
				AgentMetrics.Total.Inc(1)            // keep growing
				AgentMetrics.Bytes.Dec(rec.Bytes)    // decrement since we're done

			}
			// Add entry for remote TFC after transfer is completed
			url = fmt.Sprintf("%s/tfc", t.DstUrl)
			d, e := json.Marshal(trRecords)
			if e != nil {
				return e
			}
			resp = utils.FetchResponse(url, d) // POST request
			if resp.Error != nil {
				return resp.Error
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
