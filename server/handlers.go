package server

// transfer2go agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/vkuznet/transfer2go/model"
	"github.com/vkuznet/transfer2go/utils"
)

// GET methods

// FilesHandler provides information about files in catalog
func FilesHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	pattern := r.FormValue("pattern")
	files := model.TFC.Files(pattern)
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
	astats := AgentStatus{Addrs: addrs, Catalog: model.TFC.Type, Name: _alias, Url: _myself, Protocol: _protocol, Backend: _backend, Tool: _tool, TransferCounter: model.TransferCounter, Agents: _agents, TimeStamp: time.Now().Unix()}
	data, err := json.Marshal(astats)
	if err != nil {
		log.Println("ERROR AgentsHandler", err)
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(data)
}

// AgentsHandler serves list of known agents
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
		log.Println("WARNING RegisterHandler unable to decode", r.Body, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// register another agent
	agent := agentParams.Agent
	alias := agentParams.Alias
	if aurl, ok := _agents[alias]; ok {
		msg := fmt.Sprintf("Agent %s already exists in agent map at %s, %v\n", alias, aurl, _agents)
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(msg))
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
		log.Println("WARNING RegisterHandler unable to decode", r.Body, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// register another protocol for myself
	if stat, err := os.Stat(protocolParams.Tool); err == nil && !stat.IsDir() {
		_protocol = protocolParams.Protocol
		_backend = protocolParams.Backend
		_tool = protocolParams.Tool
		log.Printf("INFO RegisterHandler switched to protocol=%s backend=%s tool=%s\n", _protocol, _backend, _tool)
		w.WriteHeader(http.StatusOK)
		return
	} else {
		log.Println("ERROR RegisterHandler", err)
	}
	w.WriteHeader(http.StatusInternalServerError)
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
	var content = &model.TransferCollection{}
	err := json.NewDecoder(r.Body).Decode(&content)
	if err != nil {
		log.Println("ERROR RequestHandler unable to decode TransferCollection", err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// go through each request and queue items individually to run job over the given request
	for _, rdoc := range content.Requests {

		// let's create a job with the payload
		work := model.Job{TransferRequest: rdoc}

		// Push the work onto the queue.
		model.JobQueue <- work
	}

	w.WriteHeader(http.StatusOK)
}

// TransferDataHandler handles TransferData type received over HTTP
func TransferDataHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var td model.TransferData
	err := json.NewDecoder(r.Body).Decode(&td)
	if err != nil {
		log.Println("ERROR, TransferHandler unable to unmarshal incoming data", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// So far we call catalog.Uri to handle the file path and use simple file writer
	// to write directly to filesystem. Instead, I need to handle data via catalog
	if model.TFC.Type == "filesystem" {
		arr := strings.Split(td.File, "/")
		fname := arr[len(arr)-1]
		filePath := fmt.Sprintf("%s/%s", model.TFC.Uri, fname)
		err := ioutil.WriteFile(filePath, td.Data, 0666)
		if err != nil {
			log.Println("ERROR, TransferHandler unable to write file", fname)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// verify hash, bytes of transferred data
		hash, bytes := utils.Hash(td.Data)
		if hash != td.Hash {
			log.Println("ERROR, TransferHandler written file has different hash", hash, td.Hash)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if bytes != td.Bytes {
			log.Println("ERROR, TransferHandler written file has different number of bytes", bytes, td.Bytes)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Printf("wrote %s/%s %s/%s hash=%s, bytes=%v\n", td.SrcAlias, fname, td.DstAlias, filePath, td.Hash, td.Bytes)
	} else if model.TFC.Type == "sqlitedb" {
		log.Println("Not implemented")
	}
	w.WriteHeader(http.StatusOK)
}
