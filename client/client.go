package client

// transfer2go/client - Go implementation transfer2go client
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/vkuznet/transfer2go/model"
	"github.com/vkuznet/transfer2go/utils"
)

// Transfer client function is responsible to initiate transfer request from
// source to destination.
func Transfer(agent, src, dst string) error {
	fmt.Println("### Transfer", agent, src, "to site", dst)

	// find out list of all agents
	url := fmt.Sprintf("%s/agents", agent)
	resp := utils.FetchResponse(url, []byte{})
	if resp.Error != nil {
		return resp.Error
	}
	var remoteAgents map[string]string
	e := json.Unmarshal(resp.Data, &remoteAgents)
	if e != nil {
		return e
	}

	// get agent alias name
	var agentAlias string
	for alias, aurl := range remoteAgents {
		if agent == aurl {
			agentAlias = alias
			break
		}
	}

	// check if destination is ok
	dstUrl, ok := remoteAgents[dst]
	if !ok {
		fmt.Println("Unable to resolve destination", dst)
		fmt.Println("Map of known agents", remoteAgents)
		return fmt.Errorf("Unknown destination")
	}

	// Read data from source agent
	url = fmt.Sprintf("%s/files?pattern=%s", agent, src)
	resp = utils.FetchResponse(url, []byte{})
	if resp.Error != nil {
		return resp.Error
	}
	var files []string
	err := json.Unmarshal(resp.Data, &files)
	if err != nil {
		return err
	}

	// form transfer request
	source := fmt.Sprintf("%s:%s", agentAlias, agent)
	destination := fmt.Sprintf("%s:%s", dst, dstUrl)
	var requests []model.TransferRequest
	for _, fname := range files {
		ts := time.Now().Unix()
		requests = append(requests, model.TransferRequest{Source: source, Destination: destination, File: fname, TimeStamp: ts})
	}
	ts := time.Now().Unix()
	transferCollection := model.TransferCollection{TimeStamp: ts, Requests: requests}

	url = fmt.Sprintf("%s/request", dstUrl)
	d, e := json.Marshal(transferCollection)
	if e != nil {
		return e
	}
	resp = utils.FetchResponse(url, d)
	return resp.Error
}

// TransferData function is responsible to initiate transfer request from
// source to destination. So far I wrote code for transfer implementation
// on a client side, but this code should be move to a server side.
// TODO: I need to compose a TransferData JSON
func TransferData(agent, src, dst string) error {
	fmt.Println("### Transfer", agent, src, "to site", dst)

	// find out list of all agents
	url := fmt.Sprintf("%s/agents", agent)
	resp := utils.FetchResponse(url, []byte{})
	if resp.Error != nil {
		return resp.Error
	}
	var remoteAgents map[string]string
	e := json.Unmarshal(resp.Data, &remoteAgents)
	if e != nil {
		return e
	}

	// get agent alias name
	var agentAlias string
	for alias, aurl := range remoteAgents {
		if agent == aurl {
			agentAlias = alias
			break
		}
	}

	// check if destination is ok
	dstUrl, ok := remoteAgents[dst]
	if !ok {
		fmt.Println("Unable to resolve destination", dst)
		fmt.Println("Map of known agents", remoteAgents)
		return fmt.Errorf("Unknown destination")
	}

	// Read data from source agent
	url = fmt.Sprintf("%s/files?pattern=%s", agent, src)
	resp = utils.FetchResponse(url, []byte{})
	if resp.Error != nil {
		return resp.Error
	}
	var files []string
	err := json.Unmarshal(resp.Data, &files)
	if err != nil {
		return err
	}
	var data2transfer []model.TransferData
	fmt.Println("### found", agentAlias, agent, files)
	for _, fname := range files {
		// TODO: Hash function should return hash, bytes and []byte
		// then we can reading from file again
		hash, b := model.Hash(fname)
		data, err := ioutil.ReadFile(fname)
		if err == nil {
			source := fmt.Sprintf("%s:%s", agentAlias, agent)
			destination := fmt.Sprintf("%s:%s", dst, dstUrl)
			transferData := model.TransferData{Source: source, Destination: destination, Name: fname, Data: data, Hash: hash, Bytes: b}
			data2transfer = append(data2transfer, transferData)
		}
	}
	fmt.Println("### agent", agentAlias, agent, "transfer", len(data2transfer), "files")

	url = fmt.Sprintf("%s/transfer", dstUrl)
	d, e := json.Marshal(data2transfer)
	if e != nil {
		return e
	}
	resp = utils.FetchResponse(url, d)
	return resp.Error
}

// Status function provides status about given agent
func Status(agent string) error {
	resp := utils.FetchResponse(agent+"/status", []byte{})
	fmt.Println("### Status", agent, string(resp.Data))
	return resp.Error
}
