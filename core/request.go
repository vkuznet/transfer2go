package core

// transfer2go data core module, request implementation
// Author: Valentin Kuznetsov <vkuznet@gmail.com>

import (
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
	CpuUsage  float64           `json:"cpuusage"` // percentage of cpu used
	MemUsage  float64           `json:"memusage"` // Avg RAM used in MB
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
func fileTransferRequest(c CatalogEntry, tr *TransferRequest) (*http.Response, error) {
	file, err := os.Open(c.Pfn)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// Define go pipe
	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)
	var resp *http.Response
	// we need to wait for everything to be done
	done := make(chan error)
	go func() {
		url := fmt.Sprintf("%s/upload", tr.DstUrl)
		req, err := http.NewRequest("POST", url, pr)
		if err != nil {
			done <- err
			return
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())
		req.Header.Set("Pfn", c.Pfn)
		req.Header.Set("Lfn", c.Lfn)
		req.Header.Set("Bytes", fmt.Sprintf("%d", c.Bytes))
		req.Header.Set("Hash", c.Hash)
		req.Header.Set("Src", tr.SrcAlias)
		req.Header.Set("Dst", tr.DstAlias)
		client := utils.HttpClient()
		resp, err = client.Do(req)
		if err != nil {
			done <- err
			return
		}
		if resp.StatusCode != 200 {
			done <- errors.New("Status Code is not 200")
			return
		}
		done <- nil
	}()
	part, err := writer.CreateFormFile("data", filepath.Base(c.Pfn))
	if err != nil {
		return nil, err
	}
	// Use copy of writer to avoid deadlock condition
	out := io.MultiWriter(part)
	_, err = io.Copy(out, file)
	if err != nil {
		return nil, err
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	err = pw.Close()
	if err != nil {
		return nil, err
	}
	err = <-done
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// helper function to perform transfer via HTTP protocol
func httpTransfer(c CatalogEntry, t *TransferRequest) (string, float64, error) {
	start := time.Now()
	resp, err := fileTransferRequest(c, t) // create file transfer request
	elapsed := time.Since(start)
	if err != nil {
		return "", 0, err
	}
	if resp == nil || resp.StatusCode != 200 {
		return "", 0, errors.New("Empty response from destination")
	}
	defer resp.Body.Close()
	var r CatalogEntry
	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return "", 0, err
	}
	mbytes := float64(c.Bytes) / 1048576
	throughput := mbytes / elapsed.Seconds()
	return r.Pfn, throughput, nil
}

// GetRemoteFiles checks destination catalog
func GetRemoteFiles(tr TransferRequest, remote string) ([]CatalogEntry, error) {
	url := fmt.Sprintf("%s/meta", remote)
	d, err := json.Marshal(tr)
	if err != nil {
		return nil, err
	}
	resp := utils.FetchResponse(url, d)
	if resp.Error != nil {
		return nil, resp.Error
	}
	var records []CatalogEntry
	err = json.Unmarshal(resp.Data, &records)
	if err != nil {
		return nil, err
	}
	return records, nil
}

// Get the status of agent
func checkAgent(agentUrl string) error {
	url := fmt.Sprintf("%s/status", agentUrl)
	resp := utils.FetchResponse(url, []byte{})
	if resp.Error != nil {
		return resp.Error
	}
	var srcAgent AgentStatus
	err := json.Unmarshal(resp.Data, &srcAgent)
	if err != nil {
		return err
	}
	return nil
}

// SubmitRequest submits request to destination
func SubmitRequest(t []TransferRequest, dstUrl string) error {
	body, err := json.Marshal(t)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/pull", dstUrl)
	resp := utils.FetchResponse(url, body)
	// check return status code
	if resp.StatusCode != 200 {
		return fmt.Errorf("Response %s, error=%s", resp.Status, string(resp.Data))
	}
	return nil
}

// RedirectRequest function to send the request to source
func RedirectRequest(t *TransferRequest, dstUrl string) error {
	selectedAgents, index, err := AgentRouter.FindSource(t)
	if err != nil {
		return err
	}
	transferCount := 0
	for i := len(selectedAgents) - 1; i > index; i-- {
		err := checkAgent(selectedAgents[i].SrcUrl)
		if err != nil {
			log.WithFields(log.Fields{
				"Error":  err,
				"Source": selectedAgents[i].SrcUrl,
			}).Println("Unable to connect to source")
			continue
		}
		err = SubmitRequest(selectedAgents[i].Requests, dstUrl)
		if err == nil {
			transferCount += 1
		}
	}
	if transferCount == 0 {
		return errors.New("[Pull Model] Could not submit requests to destination")
	}
	return nil
}

// Compare two CatalogEntry
func compareRecords(requestedCatalog []CatalogEntry, remoteCatalog []CatalogEntry) []CatalogEntry {
	var records []CatalogEntry
	files := make(map[string]string) // Create a hashmap of files to reduce the time complexity of comparison
	for _, rec := range remoteCatalog {
		files[rec.Lfn] = rec.Hash
	}
	// check for each entry in requestedCatalog if it is present in remoteCatalog or not.
	for _, rec := range requestedCatalog {
		if _, ok := files[rec.Lfn]; !ok {
			records = append(records, rec)
		} else if rec.Hash != files[rec.Lfn] {
			records = append(records, rec)
		}
	}
	return records
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
			}
			heap.Push(&RequestQueue, item)
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

			err := TFC.UpdateRequest(t.Id, "deleted")

			if err == nil {
				deleted := RequestQueue.Delete(t.Id)
				if deleted {
					log.WithFields(log.Fields{
						"Request": t,
					}).Println("Request Deleted")
				} else {
					t.Status = "error"
					err = errors.New("Can't find request in Heap")
					return err
				}
			} else {
				t.Status = "error"
				err = errors.New("Can't find request in Database")
				return err
			}

			return r.Process(t)
		})
	}
}

// PullTransfer returns a Decorator that performs request transfers by pull model
func PullTransfer() Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			log.WithFields(log.Fields{
				"Request": t.String(),
			}).Info("Request Transfer (pull model)")
			// Here we implement the following logic:
			// - send request to src agent /download?lfn=file.root
			//   - if 204 No Content, change transfer request status to processing
			// - received data and call AgentStager to write data to local storage
			// - register newly received file into local Catalog

			// check if record exists in TFC
			existingRecords := TFC.Records(*t)
			if len(existingRecords) > 0 {
				log.WithFields(log.Fields{
					"Request": t.String(),
				}).Info("Request Transfer (pull model), no existing records in local TFC")
				return r.Process(t) // nothing to do since we have this record in TFC
			}

			// try to download a file from remote agent
			time0 := time.Now().Unix()
			url := fmt.Sprintf("%s/download?lfn=%s", t.SrcUrl, t.File)
			resp := utils.FetchResponse(url, []byte{})
			if resp.Error != nil {
				log.WithFields(log.Fields{
					"Request":             t.String(),
					"Response.Error":      resp.Error,
					"Response.Status":     resp.Status,
					"Response.StatusCode": resp.StatusCode,
				}).Error("Request Transfer (pull model), response error")
				return resp.Error
			}
			if resp.StatusCode == 204 {
				// transfer was put into stager but not yet finished
				t.Status = "processing"
				log.WithFields(log.Fields{
					"Request": t.String(),
				}).Info("Request Transfer (pull model), received 204 status code, set processing")
			}
			if resp.StatusCode == 200 {
				// we got data add record into local catalog
				data := resp.Data
				// call local stager to put data into local pool and/or tape system
				pfn, bytes, hash, err := AgentStager.Write(data, t.File)
				if err != nil {
					log.WithFields(log.Fields{
						"Request": t.String(),
						"Error":   err,
					}).Error("Request Transfer (pull model), AgentStager.Write error")
					return err
				}
				// create catalog entry for this data
				entry := CatalogEntry{Lfn: t.File, Pfn: pfn, Dataset: t.Dataset, Block: t.Block, Bytes: bytes, Hash: hash, TransferTime: (time.Now().Unix() - time0), Timestamp: time.Now().Unix()}
				// update local TFC with new catalog entry
				TFC.Add(entry)
				log.WithFields(log.Fields{
					"Request": t.String(),
					"Entry":   entry.String(),
				}).Info("Request Transfer (pull model), successfully added to this agent")
				// change status of the processed request
				t.Status = ""
				// record how much we transferred
				AgentMetrics.TotalBytes.Inc(bytes) // keep growing
				AgentMetrics.Total.Inc(1)          // keep growing
			}
			return r.Process(t)
		})
	}
}

// PushTransfer returns a Decorator that performs request transfers
func PushTransfer() Decorator {
	return func(r Request) Request {
		return RequestFunc(func(t *TransferRequest) error {
			log.WithFields(log.Fields{
				"Request": t.String(),
			}).Println("Request Transfer (push model)")
			var records []CatalogEntry
			requestedRecords := TFC.Records(*t)
			// Check if the requested data is already presented on destination agent.
			remoteRecords, err := GetRemoteFiles(*t, t.DstUrl)
			if remoteRecords == nil || err != nil {
				records = requestedRecords
			} else {
				records = compareRecords(requestedRecords, remoteRecords) // Filter the matching records
			}

			if len(records) == 0 {
				// file does not exists in TFC, nothing to do, return immediately
				log.WithFields(log.Fields{
					"TransferRequest": t,
				}).Warn("Does not match anything in TFC of this agent or data already exists in destination\n")
				return r.Process(t)
			}
			// obtain information about source and destination agents
			url := fmt.Sprintf("%s/status", t.DstUrl)
			resp := utils.FetchResponse(url, []byte{})
			if resp.Error != nil {
				return resp.Error
			}
			var dstAgent AgentStatus
			err = json.Unmarshal(resp.Data, &dstAgent)
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
			// Overwrite the previous error status
			t.Status = ""
			for _, rec := range records {

				time0 := time.Now().Unix()

				AgentMetrics.Bytes.Inc(rec.Bytes)

				// if protocol is not given use default one: HTTP
				var rpfn string // remote PFN
				var throughput float64
				if srcAgent.Protocol == "" || srcAgent.Protocol == "http" {
					log.WithFields(log.Fields{
						"dstAgent": dstAgent.String(),
					}).Info("Transfer via HTTP protocol to")
					rpfn, throughput, err = httpTransfer(rec, t)
					if err != nil {
						log.WithFields(log.Fields{
							"TransferRequest": t.String(),
							"Record":          rec.String(),
							"Err":             err,
						}).Error("Transfer", rec.String(), t.String(), err)
						t.Status = err.Error()
						continue // if we fail on single record we continue with others
					}
					cusage, memUsage, err := AgentMetrics.GetUsage()
					if err == nil {
						// store data in table
						TFC.InsertTransfers(time.Now().Unix(), cusage, memUsage, throughput)
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
					}).Info("Transfer command")
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
