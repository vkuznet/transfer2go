package server

// transfer2go agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/adler32"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"time"

	"github.com/vkuznet/transfer2go/core"
	"github.com/vkuznet/transfer2go/utils"
)

// global variable which we initialize once
var _userDNs []string

func userDNs() []string {
	var out []string
	rurl := "https://cmsweb.cern.ch/sitedb/data/prod/people"
	resp := utils.FetchResponse(rurl, []byte{})
	if resp.Error != nil {
		log.Println("ERROR unable to fetch SiteDB records", resp.Error)
		return out
	}
	var rec map[string]interface{}
	err := json.Unmarshal(resp.Data, &rec)
	if err != nil {
		log.Println("ERROR unable to unmarshal response", err)
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

func init() {
	_userDNs = userDNs()
}

// custom logic for CMS authentication, users may implement their own logic here
func auth(r *http.Request) bool {
	if utils.VERBOSE > 1 {
		dump, err := httputil.DumpRequest(r, true)
		log.Println("AuthHandler HTTP request", r, string(dump), err)
	}
	userDN := utils.UserDN(r)
	match := utils.InList(userDN, _userDNs)
	if !match {
		log.Println("ERROR Auth userDN", userDN, "not found in SiteDB")
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
	case "reset":
		ResetHandler(w, r)
	case "tfc":
		TFCHandler(w, r)
	case "upload":
		UploadDataHandler(w, r)
	case "request":
		RequestHandler(w, r)
	case "register":
		RegisterAgentHandler(w, r)
	case "protocol":
		RegisterProtocolHandler(w, r)
	default:
		DefaultHandler(w, r)
	}
}

// GET methods

// FilesHandler provides information about files in catalog
func FilesHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	lfn := r.FormValue("lfn")
	dataset := r.FormValue("dataset")
	block := r.FormValue("block")
	files := core.TFC.Files(dataset, block, lfn)
	data, err := json.Marshal(files)
	if err != nil {
		log.Println("ERROR AgentsHandler", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(data)
}

// StatusHandler provides information about the agent
func StatusHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	addrs := utils.HostIP()
	astats := core.AgentStatus{Addrs: addrs, Catalog: core.TFC.Type, Name: _alias, Url: _myself, Protocol: _protocol, Backend: _backend, Tool: _tool, ToolOpts: _toolOpts, Agents: _agents, TimeStamp: time.Now().Unix(), Metrics: core.AgentMetrics.ToDict()}
	data, err := json.Marshal(astats)
	if err != nil {
		log.Println("ERROR AgentsHandler", err)
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
		log.Println("ERROR AgentsHandler", err)
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
	log.Printf("ResetHandler switched to protocol=%s backend=%s tool=%s\n", _protocol, _backend, _tool)
	w.WriteHeader(http.StatusOK)
}

// POST methods

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
			log.Println("ERROR TFCHandler unable to marshal", records, err)
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
		log.Println("ERROR TFCHandler unable to decode", r.Body, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	for _, rec := range records {
		err = core.TFC.Add(rec)
		log.Println("TFCHandler adds", rec.String(), err)
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
		log.Println("ERROR RegisterAgentHandler unable to decode", r.Body, err)
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
		log.Println("ERROR RegisterProtocolHandler unable to decode", r.Body, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// register another protocol for myself
	if stat, err := os.Stat(protocolParams.Tool); err == nil && !stat.IsDir() {
		_protocol = protocolParams.Protocol
		_backend = protocolParams.Backend
		_tool = protocolParams.Tool
		_toolOpts = protocolParams.ToolOpts
		log.Printf("INFO RegisterProtocolHandler switched to protocol=%s backend=%s tool=%s\n", _protocol, _backend, _tool)
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
	if utils.VERBOSE > 0 {
		log.Println("RequestHandler received request", r)
	}

	// Read the body into a string for json decoding
	var requests = &[]core.TransferRequest{}
	err := json.NewDecoder(r.Body).Decode(&requests)
	if err != nil {
		log.Println("ERROR RequestHandler unable to decode []TransferRequest", err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// go through each request and queue items individually to run job over the given request
	for _, r := range *requests {

		// let's create a job with the payload
		work := core.Job{TransferRequest: r}

		// Push the work onto the queue.
		core.JobQueue <- work
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
		log.Println("HEADER UploadDataHandler", r.Header)
	}
	// create multipart reader
	mr, e := r.MultipartReader()
	if e != nil {
		log.Println("ERROR UploadDataHandler unable to establish MultipartReader", e)
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

	// create a file which we'll write
	file, e := os.Create(pfn)
	defer file.Close()
	if e != nil {
		log.Println("ERROR UploadDataHandler unable to open", pfn, e)
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
			log.Println("ERROR UploadDataHandler unable to read chunk from the stream", e)
			break
		}
		// here is pipe: mr->p->hasher->file
		reader := io.TeeReader(p, hasher)
		b, e := io.Copy(file, reader)
		// in case we don't need hasher the code would be
		// b, e := io.Copy(file, p)
		if e != nil {
			log.Println("ERROR UploadDataHandler unable to copy chunk", e)
			break
		}
		totBytes += b
	}
	if srcBytes != fmt.Sprintf("%d", totBytes) {
		log.Println("ERROR UploadDataHandler bytes mismatch", srcBytes, totBytes, e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	if srcHash != hash {
		log.Println("ERROR UploadDataHandler hash mismatch", srcHash, hash, e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// send back catalog entry which can be used for verification
	// but do not write to catalog since another end should verify first that
	// data is transferred, then it will update the TFC
	log.Printf("UploadDataHandler wrote %s:%s to %s:%s\n", srcAlias, fname, dstAlias, pfn)
	entry := core.CatalogEntry{Lfn: lfn, Pfn: pfn, Dataset: dataset, Block: block, Bytes: totBytes, Hash: hash}
	data, e := json.Marshal(entry)
	if e != nil {
		log.Println("ERROR, UploadDataHandler unable to marshal catalog entry", entry, e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}
