package server

// transfer2go agent server implementation
// Author: Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/adler32"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vkuznet/transfer2go/core"
	"github.com/vkuznet/transfer2go/utils"
)

// global variable which we initialize once
var _userDNs []string
var authVar bool

func userDNs() []string {
	var out []string
	rurl := "https://cmsweb.cern.ch/sitedb/data/prod/people"
	resp := utils.FetchResponse(rurl, []byte{})
	if resp.Error != nil {
		log.WithFields(log.Fields{
			"Error": resp.Error,
		}).Error("Unable to fetch SiteDB records", resp.Error)
		return out
	}
	var rec map[string]interface{}
	err := json.Unmarshal(resp.Data, &rec)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("Unable to unmarshal response", err)
		return out
	}
	desc := rec["desc"].(map[string]interface{})
	headers := desc["columns"].([]interface{})
	var idx int
	for i, h := range headers {
		if h.(string) == "dn" {
			idx = i
			break
		}
	}
	values := rec["result"].([]interface{})
	for _, item := range values {
		val := item.([]interface{})
		v := val[idx]
		if v != nil {
			out = append(out, v.(string))
		}
	}
	return out
}

// func init() {
//	_userDNs = userDNs()
//}

// Init is custom initialization function, we don't use init() because we want
// control of authentication from command line
func Init(authArg bool) {
	authVar = authArg
	if authVar {
		_userDNs = userDNs()
	}
}

// custom logic for CMS authentication, users may implement their own logic here
func auth(r *http.Request) bool {

	if !authVar {
		return true
	}

	if utils.VERBOSE > 1 {
		dump, err := httputil.DumpRequest(r, true)
		log.WithFields(log.Fields{
			"Request": r,
			"Dump":    string(dump),
			"Error":   err,
		}).Println("AuthHandler HTTP request")
	}
	userDN := utils.UserDN(r)
	match := utils.InList(userDN, _userDNs)
	if !match {
		log.WithFields(log.Fields{
			"User DN": userDN,
		}).Error("Auth userDN not found in SiteDB")
	}
	return match
}

// AuthHandler authenticate incoming requests and route them to appropriate handler
func AuthHandler(w http.ResponseWriter, r *http.Request) {
	// check if server started with hkey file (auth is required)
	status := auth(r)
	if !status {
		msg := "You are not allowed to access this resource"
		http.Error(w, msg, http.StatusForbidden)
		return
	}
	arr := strings.Split(r.URL.Path, "/")
	path := arr[len(arr)-1]
	switch path {
	case "status":
		StatusHandler(w, r)
	case "agents":
		AgentsHandler(w, r)
	case "files":
		FilesHandler(w, r)
	case "records":
		RecordsHandler(w, r)
	case "reset":
		ResetHandler(w, r)
	case "tfc":
		TFCHandler(w, r)
	case "snapshot":
		SnapshotHandler(w, r)
	case "catalog":
		CentralCatalogHandler(w, r)
	case "upload":
		UploadDataHandler(w, r)
	case "download":
		DownloadHandler(w, r)
	case "request":
		RequestHandler(w, r)
	case "register":
		RegisterAgentHandler(w, r)
	case "protocol":
		RegisterProtocolHandler(w, r)
	case "verbose":
		VerboseHandler(w, r)
	case "list":
		ListHandler(w, r)
	case "action":
		ActionHandler(w, r)
	case "pull":
		PullHandler(w, r)
	case "push":
		PushHandler(w, r)
	case "meta":
		MetaHandler(w, r)
	case "history":
		HistoricalHandler(w, r)
	default:
		DefaultHandler(w, r)
	}
}

// GET methods

// HistoricalHandler is endpoint to get the historical data
func HistoricalHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	d, _ := url.QueryUnescape(r.FormValue("duration"))
	duration, err := time.ParseDuration(d)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("AgentsHandler", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	endTime := time.Now().Unix()
	startTime := endTime - int64(duration.Seconds())
	transfers, err := core.TFC.GetTransfers(strconv.FormatInt(startTime, 10), strconv.FormatInt(endTime, 10))
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("AgentsHandler", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	data, err := json.Marshal(transfers)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("AgentsHandler", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// TransfersHandler provides information about files in catalog
func TransfersHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	time0, _ := url.QueryUnescape(r.FormValue("time0"))
	time1, _ := url.QueryUnescape(r.FormValue("time1"))
	transfers := core.TFC.Transfers(time0, time1)
	data, err := json.Marshal(transfers)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("AgentsHandler", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(data)
}

// FilesHandler provides information about files in catalog
func FilesHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	lfn, _ := url.QueryUnescape(r.FormValue("lfn"))
	block, _ := url.QueryUnescape(r.FormValue("block"))
	dataset, _ := url.QueryUnescape(r.FormValue("dataset"))
	files := core.TFC.Files(dataset, block, lfn)
	data, err := json.Marshal(files)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("AgentsHandler", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(data)
}

// RecordsHandler provides information about files in catalog
func RecordsHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	lfn, _ := url.QueryUnescape(r.FormValue("lfn"))
	block, _ := url.QueryUnescape(r.FormValue("block"))
	dataset, _ := url.QueryUnescape(r.FormValue("dataset"))
	req := core.TransferRequest{Lfn: lfn, Block: block, Dataset: dataset}
	records := core.TFC.Records(req)
	data, err := json.Marshal(records)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("Recordsandler", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(data)
}

// ListHandler lists all transfer Requests
func ListHandler(w http.ResponseWriter, r *http.Request) {

	query := r.URL.RawQuery
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	parameter := strings.Split(query, "=")

	if parameter[0] == "type" {
		var requests []core.TransferRequest
		if parameter[1] == "pending" {
			requests = core.RequestQueue.GetAllRequest()
		} else {
			var err error
			requests, err = core.TFC.ListRequest(parameter[1])
			if err != nil {
				log.WithFields(log.Fields{
					"Error": err,
				}).Error("ListRequest handler")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		data, err := json.Marshal(requests)
		if err != nil {
			log.WithFields(log.Fields{
				"Error": err,
			}).Error("ListRequest handler")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Could not find type url parameter"))
	}
}

// StatusHandler provides information about the agent
func StatusHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	addrs := utils.HostIP()
	cusage, musage, err := core.AgentMetrics.GetUsage()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Calculating system metrics try after some time."))
		return
	}

	astats := core.AgentStatus{Addrs: addrs, Catalog: core.TFC.Type, Name: _alias, Url: _myself, Protocol: _protocol, Backend: _backend, Tool: _tool, ToolOpts: _toolOpts, Agents: _agents, TimeStamp: time.Now().Unix(), Metrics: core.AgentMetrics.ToDict(), CpuUsage: cusage, MemUsage: musage}
	data, err := json.Marshal(astats)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("AgentsHandler")
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(data)
}

// AgentsHandler returns list of known agents
func AgentsHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	data, err := json.Marshal(_agents)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("AgentsHandler", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(data)
}

// DefaultHandler provides information about the agent
func DefaultHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	// TODO: implement here default page for data-service
	// should be done via templates
	msg := fmt.Sprintf("Default page: %v\nagents: %v\n", time.Now(), _agents)
	w.Write([]byte(msg))
}

// ResetHandler resets current agent with default protocol and null backend/tool attributes
func ResetHandler(w http.ResponseWriter, r *http.Request) {
	_protocol = "http"
	_backend = ""
	_tool = ""
	log.WithFields(log.Fields{
		"Protocol": _protocol,
		"Backend":  _backend,
		"Tool":     _tool,
	}).Printf("ResetHandler switched to")
	w.WriteHeader(http.StatusOK)
}

// POST methods

// MetaHandler gives details about requested dataset/block/files
func MetaHandler(w http.ResponseWriter, r *http.Request) {

	if !(r.Method == "POST") {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var request = core.TransferRequest{}
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("MetaHandler unable to marshal", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	lfn := request.Lfn
	block := request.Block
	dataset := request.Dataset
	records := core.TFC.Records(core.TransferRequest{Lfn: lfn, Dataset: dataset, Block: block})
	data, err := json.Marshal(records)
	if err != nil {
		log.WithFields(log.Fields{
			"Records": records,
			"Error":   err,
		}).Error("MetaHandler unable to marshal", records, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
	return
}

// PullHandler handles pull acknowledge message from main agent.
func PullHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var data []core.TransferRequest
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("PullHandler unable to parse json body")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var jobs []core.Job
	for _, req := range data {
		jobs = append(jobs, core.Job{TransferRequest: req, Action: "transfer"})
	}
	body, err := json.Marshal(jobs)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
			"jobs":  jobs,
		}).Error("PullHandler unable to marshall jobs")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(data) > 0 {
		url := fmt.Sprintf("%s/action", data[0].DstUrl)
		resp := utils.FetchResponse(url, body)
		// Check return status code
		if resp.StatusCode != 200 {
			log.WithFields(log.Fields{
				"Error":       err,
				"Destination": data[0].DstUrl,
			}).Error("PullHandler unable to send transfer request to destination agent")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	return
}

// PushHandler performs pushing data from a source to destination agent
func PushHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var data []core.TransferRequest
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("PushHandler unable to parse json body")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	var jobs []core.Job
	for _, req := range data {
		jobs = append(jobs, core.Job{TransferRequest: req, Action: "transfer"})
	}
	body, err := json.Marshal(jobs)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
			"jobs":  jobs,
		}).Error("PushHandler unable to marshal jobs")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if len(data) > 0 {
		url := fmt.Sprintf("%s/action", data[0].SrcUrl)
		resp := utils.FetchResponse(url, body)
		// Check return status code
		if resp.StatusCode != 200 {
			log.WithFields(log.Fields{
				"Error":  err,
				"Source": data[0].SrcUrl,
			}).Error("PushHandler unable to send transfer request to source agent")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	return
}

// ActionHandler handles operations on requests
func ActionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var data = []core.Job{}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("ActionHandler unable to parse json body")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// loop over received data and decide what to do with those requests based on the action clause
	for _, job := range data {
		log.WithFields(log.Fields{
			"Request": job.String(),
		}).Info("ActionHandler, receive new request")
		if job.Action == "approve" { // this is action happens on main agent
			// find out real transfer request
			err := core.TFC.RetrieveRequest(&job.TransferRequest)
			if err != nil {
				log.WithFields(log.Fields{
					"Job":   job,
					"Error": err,
				}).Error("ActionHandler, unable to find a request")
				continue
			}
			// change it action to transfer and get destination agent url
			job.Action = "transfer"
			job.TransferRequest.Status = "processing"
			agent := job.TransferRequest.DstUrl // pull model
			if _config.Type == "push" {
				agent = job.TransferRequest.SrcUrl // push model
			}
			if agent == "" {
				log.WithFields(log.Fields{
					"Job": job,
				}).Error("ActionHandler, undefined agent")
				continue
			}
			var jobs4Agent []core.Job
			jobs4Agent = append(jobs4Agent, job)
			furl := fmt.Sprintf("%s/action", agent)
			d, err := json.Marshal(jobs4Agent)
			if err != nil {
				log.WithFields(log.Fields{
					"Error": err,
				}).Error("ActionHandler unable to marshal jobs4Agent")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			// send request to destination agent
			resp := utils.FetchResponse(furl, d)
			if resp.StatusCode != 200 {
				log.WithFields(log.Fields{
					"Error": err,
					"Agent": agent,
				}).Error("ActionHandler unable to send transfer request to agent")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			log.WithFields(log.Fields{
				"Job":   job.String(),
				"Agent": agent,
			}).Info("ActionHandler, successfully send request to agent")
		} else if job.Action == "update" { // this happens on main agent
			err := core.TFC.UpdateRequest(job.TransferRequest.Id, job.TransferRequest.Status)
			if err == nil {
				core.RequestQueue.Delete(job.TransferRequest.Id) // Remove request from heap.
			}
		} else { // this action happens either on source or destination agent
			// we put received job into transfer queue
			core.TransferQueue <- job
		}
	}
	w.WriteHeader(http.StatusOK)
}

// SnapshotHandler return all records in our catalog as JSON structure
// the JSON is a map of table names and rows where each row is represented
// as comma separated list of values. This handler can be used by CentralCatalog
// to fetch data from individual agents
func SnapshotHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	records := core.TFC.Snapshot()
	data, err := json.Marshal(records)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("SnapshotHandler unable to marshal records")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// CentralCatalogHandler return all records in our catalog
func CentralCatalogHandler(w http.ResponseWriter, r *http.Request) {
	if !(r.Method == "POST" || r.Method == "GET") {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	if core.CC.Path == "" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method == "GET" {
		// should return records from central catalog or its subset
		table, _ := url.QueryUnescape(r.FormValue("table"))
		data, err := core.CC.Get(table)
		if err != nil {
			log.WithFields(log.Fields{
				"Table": table,
				"Error": err,
			}).Error("CentralCatalogHandler unable to read table")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	} else if r.Method == "POST" {
		// should put snapshot of agent catalog into central catalog
		var maps map[string][]string
		err := json.NewDecoder(r.Body).Decode(&maps)
		if err != nil {
			log.WithFields(log.Fields{
				"Request Body": r.Body,
				"Error":        err,
			}).Error("RegisterAgentHandler unable to decode")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		for table, records := range maps {
			err := core.CC.Put(table, records)
			if err != nil {
				log.WithFields(log.Fields{
					"Error": err,
					"Table": table,
				}).Error("CentralCatalogHandler unable to insert records into table")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// TFCHandler registers given record in local TFC
func TFCHandler(w http.ResponseWriter, r *http.Request) {
	if !(r.Method == "POST" || r.Method == "GET") {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	if r.Method == "GET" {
		records := core.TFC.Records(core.TransferRequest{})
		data, err := json.Marshal(records)
		if err != nil {
			log.WithFields(log.Fields{
				"Error": err,
			}).Error("TFCHandler unable to marshal records")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(data)
		return
	}
	var records []core.CatalogEntry
	err := json.NewDecoder(r.Body).Decode(&records)
	if err != nil {
		log.WithFields(log.Fields{
			"Request Body": r.Body,
			"Error":        err,
		}).Error("TFCHandler unable to decode", r.Body, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	for _, rec := range records {
		err = core.TFC.Add(rec)
		log.WithFields(log.Fields{
			"Record": rec.String(),
			"Error":  err,
		}).Println("TFCHandler adds")
	}
	w.WriteHeader(http.StatusOK)
}

// RegisterAgentHandler registers current agent with another one
func RegisterAgentHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var agentParams AgentInfo
	err := json.NewDecoder(r.Body).Decode(&agentParams)
	if err != nil {
		log.WithFields(log.Fields{
			"Request Body": r.Body,
			"Error":        err,
		}).Error("RegisterAgentHandler unable to decode")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// register another agent
	agent := agentParams.Agent
	alias := agentParams.Alias
	if aurl, ok := _agents[alias]; ok {
		if aurl != agent {
			msg := fmt.Sprintf("Agent %s (%s) already exists in agents map, %v\n", alias, aurl, _agents)
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(msg))
			return
		}
		w.WriteHeader(http.StatusOK)
	} else {
		_agents[alias] = agent // register given agent/alias pair internally
		w.WriteHeader(http.StatusOK)
	}
}

// RegisterProtocolHandler registers current agent with another one
func RegisterProtocolHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var protocolParams AgentProtocol
	err := json.NewDecoder(r.Body).Decode(&protocolParams)
	if err != nil {
		log.WithFields(log.Fields{
			"Request Body": r.Body,
			"Error":        err,
		}).Error("RegisterProtocolHandler unable to decode", r.Body, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// register another protocol for myself
	if stat, err := os.Stat(protocolParams.Tool); err == nil && !stat.IsDir() {
		_protocol = protocolParams.Protocol
		_backend = protocolParams.Backend
		_tool = protocolParams.Tool
		_toolOpts = protocolParams.ToolOpts
		log.WithFields(log.Fields{
			"Protocol": _protocol,
			"Backend":  _backend,
			"Tool":     _tool,
		}).Println("RegisterProtocolHandler switched to")
		w.WriteHeader(http.StatusOK)
		return
	}
}

// RequestHandler initiate transfer work for given request
func RequestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	// Read the body into a string for json decoding
	var requests = &[]core.TransferRequest{}
	err := json.NewDecoder(r.Body).Decode(&requests)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("RequestHandler unable to decode []TransferRequest", err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// go through each request and queue items individually to run job over the given request
	for _, r := range *requests {
		log.WithFields(log.Fields{
			"Request": r,
		}).Info("RequestHandler received request")

		// this action will cause main agent to store given request in heap and persistent storage (REQUEST table)
		r.Id = r.UUID()
		work := core.Job{TransferRequest: r, Action: "store"}

		// Push the work onto the queue.
		core.StorageQueue <- work
	}

	w.WriteHeader(http.StatusOK)
}

// UploadDataHandler upload TransferRecord record and send back catalog entry to recipient
// http://sanatgersappa.blogspot.com/2013/03/handling-multiple-file-uploads-in-go.html
func UploadDataHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	if utils.VERBOSE > 0 {
		log.WithFields(log.Fields{
			"Header": r.Header,
		}).Println("HEADER UploadDataHandler", r.Header)
	}
	// create multipart reader
	mr, e := r.MultipartReader()
	if e != nil {
		log.WithFields(log.Fields{
			"Error": e,
		}).Error("UploadDataHandler unable to establish MultipartReader", e)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// parse header values and extract transfer record meta-data
	srcBytes := r.Header.Get("Bytes")
	srcHash := r.Header.Get("Hash")
	dataset := r.Header.Get("Dataset")
	block := r.Header.Get("Block")
	srcAlias := r.Header.Get("Src")
	dstAlias := r.Header.Get("Dst")
	lfn := r.Header.Get("Lfn")
	arr := strings.Split(lfn, "/")
	fname := arr[len(arr)-1]
	pfn := fmt.Sprintf("%s/%s", _backend, fname)
	time0 := time.Now().Unix()

	// create a file which we'll write
	file, e := os.Create(pfn)
	defer file.Close()
	if e != nil {
		log.WithFields(log.Fields{
			"PFN":   pfn,
			"Error": e,
		}).Error("ERROR UploadDataHandler unable to open", pfn, e)
		http.Error(w, e.Error(), http.StatusInternalServerError)
		return
	}
	// create a hasher to calculate data hash
	hasher := adler32.New()

	// loop over parts of HTTP request, pass it through TeeReader to destination file and collect bytes
	var totBytes int64
	for {
		p, e := mr.NextPart()
		if e == io.EOF {
			break
		}
		if p.FileName() == "" {
			continue
		}
		if e != nil {
			log.WithFields(log.Fields{
				"Error": e,
			}).Error("UploadDataHandler unable to read chunk from the stream", e)
			break
		}
		// here is pipe: mr->p->hasher->file
		reader := io.TeeReader(p, hasher)
		b, e := io.Copy(file, reader)
		// in case we don't need hasher the code would be
		// b, e := io.Copy(file, p)
		if e != nil {
			log.WithFields(log.Fields{
				"Error": e,
			}).Error("UploadDataHandler unable to copy chunk", e)
			break
		}
		totBytes += b
	}
	if srcBytes != fmt.Sprintf("%d", totBytes) {
		log.WithFields(log.Fields{
			"Source Bytes": srcBytes,
			"Total Bytes":  totBytes,
			"Error":        e,
		}).Error("UploadDataHandler bytes mismatch")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	if srcHash != hash {
		log.WithFields(log.Fields{
			"Source Hash": srcHash,
			"Hash":        hash,
			"Error":       e,
		}).Error("UploadDataHandler hash mismatch")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// send back catalog entry which can be used for verification
	// but do not write to catalog since another end should verify first that
	// data is transferred, then it will update the TFC
	log.WithFields(log.Fields{
		"Source Alias": srcAlias,
		"Fname":        fname,
		"Dest Alias":   dstAlias,
		"PFN":          pfn,
	}).Println("UploadDataHandler wrote")
	entry := core.CatalogEntry{Lfn: lfn, Pfn: pfn, Dataset: dataset, Block: block, Bytes: totBytes, Hash: hash, TransferTime: (time.Now().Unix() - time0), Timestamp: time.Now().Unix()}
	data, e := json.Marshal(entry)
	if e != nil {
		log.WithFields(log.Fields{
			"Entry": entry,
			"Error": e,
		}).Error("UploadDataHandler unable to marshal catalog entry", entry, e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// DownloadHandler handles download agent's request
func DownloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	args := r.URL.Query()
	if files, ok := args["lfn"]; ok {
		if core.AgentStager.Exist(files[0]) {
			var fin *os.File
			fname := core.AgentStager.Access(files[0])
			fin, err := os.Open(fname)
			if err != nil {
				log.WithFields(log.Fields{
					"Error": err,
					"File":  fname,
				}).Error("unable to open file in stager")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			// we don't need to WriteHeader here since it is handled by http.ServeContent
			http.ServeContent(w, r, fname, time.Now(), fin)
			return
		}
		core.AgentStager.Stage(files[0])
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.WriteHeader(http.StatusBadRequest)
}

// helper data structure to change verbosity level of the running server
type level struct {
	Level int `json:"level"`
}

// VerboseHandler sets verbosity level for the server
func VerboseHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Warn("Unable to parse request body: ", err)
	}
	var v level
	err = json.Unmarshal(body, &v)
	if err == nil {
		log.Info("Switch to verbose level: ", v.Level)
		utils.VERBOSE = v.Level
	}
	w.WriteHeader(http.StatusOK)
}
