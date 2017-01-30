package client

// transfer2go/client - Go implementation transfer2go client
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/vkuznet/transfer2go/model"
)

// VERBOSE variable control verbosity level of client's utilities
var VERBOSE int

// TransferData struct holds all attributes of transfering data, such as name, checksum, data, etc.
type TransferData struct {
	Source      string `json:source`
	Destination string `json:destination`
	Name        string `json:name`
	Data        []byte `json:data`
	Hash        string `json:hash`
	Bytes       int64  `json:bytes`
}

// Hash implements hash function for given file name, it returns a hash and number of bytes in a file
// TODO: Hash function should return hash, bytes and []byte to avoid overhead with
// reading file multiple times
func Hash(fname string) (string, int64) {
	hasher := sha256.New()
	f, err := os.Open(fname)
	if err != nil {
		msg := fmt.Sprintf("Unable to open file %s, %v", fname, err)
		panic(msg)
	}
	defer f.Close()
	b, err := io.Copy(hasher, f)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(hasher.Sum(nil)), b
}

// Transfer client function is responsible to initiate transfer request from
// source to destination.
func Transfer(agent, src, dst string) error {
	fmt.Println("### Transfer", agent, src, "to site", dst)

	// find out list of all agents
	url := fmt.Sprintf("%s/agents", agent)
	resp := FetchResponse(url, []byte{})
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
	resp = FetchResponse(url, []byte{})
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

	url = fmt.Sprintf("%s", dstUrl)
	d, e := json.Marshal(transferCollection)
	if e != nil {
		return e
	}
	resp = FetchResponse(url, d)
	return resp.Error
}

// TransferClientBased function is responsible to initiate transfer request from
// source to destination. So far I wrote code for transfer implementation
// on a client side, but this code should be move to a server side.
// TODO: I need to compose a TransferData JSON
func TransferClientBased(agent, src, dst string) error {
	fmt.Println("### Transfer", agent, src, "to site", dst)

	// find out list of all agents
	url := fmt.Sprintf("%s/agents", agent)
	resp := FetchResponse(url, []byte{})
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
	resp = FetchResponse(url, []byte{})
	if resp.Error != nil {
		return resp.Error
	}
	var files []string
	err := json.Unmarshal(resp.Data, &files)
	if err != nil {
		return err
	}
	var data2transfer []TransferData
	fmt.Println("### found", agentAlias, agent, files)
	for _, fname := range files {
		// TODO: Hash function should return hash, bytes and []byte
		// then we can reading from file again
		hash, b := Hash(fname)
		data, err := ioutil.ReadFile(fname)
		if err == nil {
			source := fmt.Sprintf("%s:%s", agentAlias, agent)
			destination := fmt.Sprintf("%s:%s", dst, dstUrl)
			transferData := TransferData{Source: source, Destination: destination, Name: fname, Data: data, Hash: hash, Bytes: b}
			data2transfer = append(data2transfer, transferData)
		}
	}
	fmt.Println("### agent", agentAlias, agent, "transfer", len(data2transfer), "files")

	url = fmt.Sprintf("%s/transfer", dstUrl)
	d, e := json.Marshal(data2transfer)
	if e != nil {
		return e
	}
	resp = FetchResponse(url, d)
	return resp.Error
}

// Status function provides status about given agent
func Status(agent string) error {
	resp := FetchResponse(agent+"/status", []byte{})
	fmt.Println("### Status", agent, string(resp.Data))
	return resp.Error
}
