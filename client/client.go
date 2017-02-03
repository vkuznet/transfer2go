package client

// transfer2go/client - Go implementation transfer2go client
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/vkuznet/transfer2go/common"
	"github.com/vkuznet/transfer2go/model"
	"github.com/vkuznet/transfer2go/utils"
)

// UserRequest represent client request to the agent
type UserRequest struct {
	SrcUrl   string
	SrcAlias string
	SrcFile  string
	DstUrl   string
	DstAlias string
	DstFile  string
	Transfer bool
	Upload   bool
}

// String returns string representation of UserRequest struct
func (u *UserRequest) String() string {
	var action string
	if u.Transfer {
		action = "transfer"
	}
	if u.Upload {
		action = "upload"
	}
	return fmt.Sprintf("<UserRequest %s %s(%s) %s => %s(%s) %s>", action, u.SrcAlias, u.SrcUrl, u.SrcFile, u.DstAlias, u.DstUrl, u.DstFile)
}

// helper function to find an LFN in agent list, return agent name/url which has this file
func findFile(agents map[string]string, file string) (string, string, string, error) {
	// agent parameters which we assign if we find a file
	var agentAlias, agentUrl string

	out := make(chan utils.ResponseType)
	defer close(out)
	umap := map[string]int{}
	for _, aurl := range agents {
		furl := fmt.Sprintf("%s/files?pattern=%s", aurl, file)
		go utils.Fetch(furl, []byte{}, out)
	}
	var aurls []string
	exit := false
	for {
		select {
		case r := <-out:
			if r.Error == nil {
				var files []string
				err := json.Unmarshal(r.Data, &files)
				if err == nil && utils.InList(file, files) {
					aurls = append(aurls, strings.Split(r.Url, "/files")[0])
				}
			}
			delete(umap, r.Url) // remove Url from map
		default:
			if len(umap) == 0 { // no more requests, merge data records
				exit = true
			}
			time.Sleep(time.Duration(10) * time.Millisecond) // wait for response
		}
		if exit {
			break
		}
	}
	if len(aurls) > 0 {
		log.Println("File", file, "found on", aurls)
	} else {
		err := fmt.Errorf("Unable to find %s in agents %v\n", file, agents)
		return "", "", "", err

	}
	// TODO: we may have intelligent logic here to pick best agent to serve, e.g. based on requests transfers
	// so far use first available one
	var load int32
	for alias, aurl := range agents {
		url := fmt.Sprintf("%s/status", aurl)
		resp := utils.FetchResponse(url, []byte{})
		if resp.Error == nil {
			var agentStatus common.AgentStatus
			err := json.Unmarshal(resp.Data, &agentStatus)
			if err != nil {
				continue
			}
			if load == -1 {
				load = agentStatus.TransferCounter
			}
			if agentStatus.TransferCounter <= load {
				agentAlias = alias
				agentUrl = aurl
			}
		}

	}
	if agentAlias == "" {
		err := fmt.Errorf("Unable to find %s in agents %v\n", file, agents)
		return agentAlias, agentUrl, file, err
	}
	return agentAlias, agentUrl, file, nil
}

// helper function to parse source and destination parameters
func parse(agent, src, dst string) (UserRequest, error) {
	var req UserRequest
	var transfer, upload bool
	var srcFile, srcAlias, srcUrl, dstFile, dstAlias, dstUrl string
	if stat, err := os.Stat(src); err == nil && !stat.IsDir() {
		// local file, we need to upload it to the destination
		upload = true
		srcFile = src
		srcUrl = agent
	}
	// start with desination
	if strings.Contains(dst, ":") {
		arr := strings.Split(dst, ":")
		dstAlias = arr[0]
		dstFile = arr[1]
	} else {
		dstAlias = dst
		dstFile = srcFile
	}
	if !upload {
		transfer = true
	}

	// find out list of all agents
	url := fmt.Sprintf("%s/agents", agent)
	resp := utils.FetchResponse(url, []byte{})
	if resp.Error != nil {
		return req, resp.Error
	}
	var remoteAgents map[string]string
	e := json.Unmarshal(resp.Data, &remoteAgents)
	if e != nil {
		return req, e
	}

	// resolve source agent name/alias and identify file to transfer
	if strings.Contains(src, ":") {
		arr := strings.Split(src, ":")
		srcAlias = arr[0]
		srcFile = arr[1]
	} else {
		// input source didn't specified site name, we should try to find a file on all agents
		// don't use := here since we assign values to srcAlias, etc., therefore I must reuse
		// error variable
		srcAlias, srcUrl, srcFile, e = findFile(remoteAgents, src)
		if e != nil {
			return req, e
		}
	}
	// get source agent alias name
	if srcAlias == "" {
		for alias, aurl := range remoteAgents {
			if agent == aurl {
				srcAlias = alias
				break
			}
		}
	}
	// get source agent url
	if srcUrl == "" {
		for alias, aurl := range remoteAgents {
			if srcAlias == alias {
				srcUrl = aurl
				break
			}
		}
	}

	// check if destination is ok
	dstUrl, ok := remoteAgents[dstAlias]
	if !ok {
		log.Println("Unable to resolve destination", dst)
		log.Println("Map of known agents", remoteAgents)
		return req, fmt.Errorf("Unknown destination")
	}
	req = UserRequest{SrcUrl: srcUrl, SrcAlias: srcAlias, SrcFile: srcFile, DstUrl: dstUrl, DstAlias: dstAlias, DstFile: dstFile, Transfer: transfer, Upload: upload}
	log.Println(req.String())
	return req, nil
}

// helper function to perform user request transfer
func transfer(req UserRequest) error {

	// Read data from source agent
	url := fmt.Sprintf("%s/files?pattern=%s", req.SrcUrl, req.SrcFile)
	resp := utils.FetchResponse(url, []byte{})
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
		requests = append(requests, model.TransferRequest{SrcUrl: req.SrcUrl, SrcAlias: req.SrcAlias, DstUrl: req.DstUrl, DstAlias: req.DstAlias, File: fname, TimeStamp: ts})
	}
	ts := time.Now().Unix()
	transferCollection := model.TransferCollection{TimeStamp: ts, Requests: requests}

	url = fmt.Sprintf("%s/request", req.SrcUrl)
	d, e := json.Marshal(transferCollection)
	if e != nil {
		return e
	}
	resp = utils.FetchResponse(url, d)
	return resp.Error
}

// helper function to perform user request upload to the agent
func upload(req UserRequest) error {
	data, err := ioutil.ReadFile(req.SrcFile)
	if err != nil {
		return err
	}
	hash, bytes := utils.Hash(data)
	d := "/a/b/c" // dataset name
	b := "123"    // block name
	transferData := model.TransferData{File: req.DstFile, Dataset: d, Block: b, SrcUrl: req.SrcUrl, SrcAlias: req.SrcAlias, DstUrl: req.DstUrl, DstAlias: req.DstAlias, Data: data, Hash: hash, Bytes: bytes}
	url := fmt.Sprintf("%s/upload", req.DstUrl)
	data, err = json.Marshal(transferData)
	if err != nil {
		return err
	}
	resp := utils.FetchResponse(url, data)
	return resp.Error
}

// Transfer client function is responsible to initiate transfer request from
// source to destination.
func Transfer(agent, src, dst string) error {
	req, err := parse(agent, src, dst)
	if err != nil {
		return err
	}

	if req.Transfer {
		return transfer(req)
	}

	if req.Upload {
		return upload(req)
	}

	return fmt.Errorf("Unable to understand client request, src=%v to dst=%v", src, dst)
}

// Status function provides status about given agent
func Status(agent string) error {
	resp := utils.FetchResponse(agent+"/status", []byte{})
	log.Println("Status", agent, string(resp.Data))
	return resp.Error
}
