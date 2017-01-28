// transfer2go agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

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
		log.Println("ERROR, unable to unmarshal params %v", params)
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
