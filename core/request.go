package core

// transfer2go data core module, request implementation
// Author: Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
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

// Store returns a Decorator that stores request
func Store() Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			t.Id = time.Now().Unix()
			item := &Item{
				Value:    *t,
				priority: t.Priority,
			}
			fmt.Println(*t)
			err := TFC.InsertRequest(*t)
			if err != nil {
				return err
			} else {
				heap.Push(&RequestQueue, item)
			}
			log.WithFields(log.Fields{
				"Request": t,
			}).Println("Request Saved")
			return r.Process(t)
		})
	}
}

// Delete returns a Decorator that deletes request from heap
func Delete() Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			// Delete request from PriorityQueue. The complexity is O(n) where n = heap.Len()
			index := -1
			var err error

			for _, item := range RequestQueue {
				if item.Value.Id == t.Id {
					index = item.index
					break
				}
			}

			if index < RequestQueue.Len() && index >= 0 {
				err = TFC.UpdateRequest(t.Id, "deleted")
				if err != nil {
					t.Status = "error"
					return err
				} else {
					// TODO: May be we need to add lock over here.
					heap.Remove(&RequestQueue, index)
					t.Status = "deleted"
					log.WithFields(log.Fields{
						"Request": t,
					}).Println("Request Deleted")
				}
			} else {
				t.Status = "error"
				err = errors.New("Can't find request in heap")
				return err
			}

			return r.Process(t)
		})
	}
}

// Transfer returns a Decorator that performs request transfers by pull model
func PullTransfer() Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			log.WithFields(log.Fields{
				"Request": t.String(),
			}).Println("Request Transfer")
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
			// if both are up then send acknowledge message to destination on /pullack url.
			body, err := json.Marshal(t)
			if err != nil {
				return err
			}
			url = fmt.Sprintf("%s/pull", t.DstUrl)
			resp = utils.FetchResponse(url, body)
			// check return status code
			if resp.StatusCode != 200 {
				return fmt.Errorf("Response %s, error=%s", resp.Status, string(resp.Data))
			}
			return r.Process(t)
		})
	}
}

// Transfer returns a Decorator that performs request transfers
func PushTransfer() Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			log.WithFields(log.Fields{
				"Request": t.String(),
			}).Println("Request Transfer")
			var records []CatalogEntry
			// Consider those requests which are failed in previous iteration.
			// If it is nil then request must be passing through first iteration.
			if t.FailedRecords != nil {
				records = t.FailedRecords
			} else {
				records = TFC.Records(*t)
			}
			if len(records) == 0 {
				// file does not exists in TFC, nothing to do, return immediately
				log.WithFields(log.Fields{
					"TransferRequest": t,
				}).Warn("Does not match anything in TFC of this agent\n", t)
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
			var failedRecords []CatalogEntry
			// Overwrite the previous error status
			t.Status = ""
			for _, rec := range records {

				time0 := time.Now().Unix()

				AgentMetrics.Bytes.Inc(rec.Bytes)

				// if protocol is not given use default one: HTTP
				var rpfn string // remote PFN
				if srcAgent.Protocol == "" || srcAgent.Protocol == "http" {
					log.WithFields(log.Fields{
						"dstAgent": dstAgent.String(),
					}).Println("Transfer via HTTP protocol to", dstAgent.String())
					rpfn, err = httpTransfer(rec, t)
					if err != nil {
						log.WithFields(log.Fields{
							"TransferRequest": t.String(),
							"Record":          rec.String(),
							"Err":             err,
						}).Error("Transfer", rec.String(), t.String(), err)
						t.Status = err.Error()
						failedRecords = append(failedRecords, rec)
						continue // if we fail on single record we continue with others
					}
				} else {
					// construct remote PFN by using destination agent backend and record LFN
					rpfn = fmt.Sprintf("%s%s", dstAgent.Backend, rec.Lfn)
					// perform transfer with the help of backend tool
					var cmd *exec.Cmd
					if srcAgent.ToolOpts == "" {
						cmd = exec.Command(srcAgent.Tool, rec.Pfn, rpfn)
					} else {
						cmd = exec.Command(srcAgent.Tool, srcAgent.ToolOpts, rec.Pfn, rpfn)
					}
					log.WithFields(log.Fields{
						"Command": cmd,
					}).Println("Transfer command")
					err = cmd.Run()
					if err != nil {
						log.WithFields(log.Fields{
							"Tool":         srcAgent.Tool,
							"Tool options": srcAgent.ToolOpts,
							"PFN":          rec.Pfn,
							"Remote PFN":   rpfn,
							"Err":          err,
						}).Error("Transfer")
						t.Status = err.Error()
						failedRecords = append(failedRecords, rec)
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
			t.FailedRecords = failedRecords
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
				log.WithFields(log.Fields{
					"Request":  t,
					"Interval": interval,
				}).Println("TransferRequest is paused by")
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
			log.WithFields(log.Fields{
				"TransferRequest": t,
			}).Println("Trace")
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
