// transfer2go agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package server

import (
	"encoding/json"
	"fmt"
	"github.com/vkuznet/transfer2go/client"
	"github.com/vkuznet/transfer2go/model"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

// FilesHandler provides information about files in catalog
func FilesHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	pattern := r.FormValue("pattern")
	files := _catalog.Files(pattern)
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
	w.WriteHeader(http.StatusOK)
	msg := fmt.Sprintf("Status content: %v\nagents: %v\n", time.Now(), _agents)
	w.Write([]byte(msg))
}

// RegisterHandler registers current agent with another one
func RegisterHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var params AgentInfo
	err := json.NewDecoder(r.Body).Decode(&params)
	if err != nil {
		log.Println("ERROR, RegisterHandler unable to unmarshal params %v", params)
	}
	agent := params.Agent
	alias := params.Alias
	if _, ok := _agents[alias]; !ok {
		_agents[alias] = agent // register given agent/alias pair internally
	}

	w.WriteHeader(http.StatusOK)
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

// RequestHandler initiate transfer work for given request
func RequestHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	if client.VERBOSE > 0 {
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

// TransferClientBasedHandler performs file transfer
// TODO: so far it is implementation of writing transfer chunk
// from one end to another. Instead, TransferHandler should handle
// transfer requests, it will get a request to transfer, the TransferData JSON
// and fire up go-routine in worker node to initiate the transfer.
func TransferClientBasedHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var transferData []client.TransferData
	err := json.NewDecoder(r.Body).Decode(&transferData)
	if err != nil {
		log.Println("ERROR, TransferHandler unable to unmarshal incoming data %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	for _, chunk := range transferData {
		arr := strings.Split(chunk.Name, "/")
		fname := arr[len(arr)-1]
		filePath := fmt.Sprintf("%s/%s", _catalog.Uri, fname)
		err := ioutil.WriteFile(filePath, chunk.Data, 0666)
		if err != nil {
			log.Println("ERROR, TransferHandler unable to write file", fname)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// read back the file and verify its hash
		hash, bytes := client.Hash(filePath)
		if hash != chunk.Hash {
			log.Println("ERROR, TransferHandler written file has different hash", hash, chunk.Hash)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if bytes != chunk.Bytes {
			log.Println("ERROR, TransferHandler written file has different number of bytes", bytes, chunk.Bytes)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Printf("wrote %s/%s %s/%s hash=%s, bytes=%v\n", chunk.Source, fname, chunk.Destination, filePath, chunk.Hash, chunk.Bytes)
	}
	w.WriteHeader(http.StatusOK)
}
