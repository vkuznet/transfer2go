package client

// transfer2go/client - Go implementation transfer2go client
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"encoding/json"
	"fmt"
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
	var requests []model.TransferRequest
	for _, fname := range files {
		ts := time.Now().Unix()
		requests = append(requests, model.TransferRequest{SrcUrl: agent, SrcAlias: agentAlias, DstUrl: dstUrl, DstAlias: dst, File: fname, TimeStamp: ts})
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

// Status function provides status about given agent
func Status(agent string) error {
	resp := utils.FetchResponse(agent+"/status", []byte{})
	fmt.Println("### Status", agent, string(resp.Data))
	return resp.Error
}
