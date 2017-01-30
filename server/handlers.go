// transfer2go agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package server

import (
	"encoding/json"
	"fmt"
	"github.com/vkuznet/transfer2go/client"
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

// TransferHandler performs file transfer
func TransferHandler(w http.ResponseWriter, r *http.Request) {

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
	} else {
		w.WriteHeader(http.StatusOK)
	}
	for _, chunk := range transferData {
		arr := strings.Split(chunk.Name, "/")
		fname := arr[len(arr)-1]
		filePath := fmt.Sprintf("%s/%s", _catalog.Uri, fname)
		err := ioutil.WriteFile(filePath, chunk.Data, 0666)
		fmt.Println("### Write", fname, filePath, err)
		if err != nil {
			log.Println("ERROR, TransferHandler unable to write file", fname)
		}
	}
}
