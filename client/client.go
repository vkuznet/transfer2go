package client

// transfer2go/client - Go implementation transfer2go client
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/vkuznet/transfer2go/model"
	"github.com/vkuznet/transfer2go/utils"
)

// AgentFiles holds agent alias/url and list of files to transfer
type AgentFiles struct {
	Alias string
	Url   string
	Files []string
}

// helper function to find agent alias from its url
func findAlias(agents map[string]string, aurl string) string {
	for alias, agent := range agents {
		if agent == aurl {
			return alias
		}
	}
	return ""
}

// helper function to rearrange given AgentFiles list into new one which merge
// files from the same agent
func reArrange(agentFiles []AgentFiles) []AgentFiles {
	var out []AgentFiles
	agents := make(map[string]string)
	afiles := make(map[string][]string)
	for _, rec := range agentFiles {
		agents[rec.Alias] = rec.Url
		eFiles, ok := afiles[rec.Alias]
		if ok {
			for _, f := range rec.Files {
				eFiles = append(eFiles, f)
			}
			afiles[rec.Alias] = eFiles
		} else {
			afiles[rec.Alias] = rec.Files
		}
	}
	for alias, aurl := range agents {
		rec := AgentFiles{Alias: alias, Url: aurl, Files: afiles[alias]}
		out = append(out, rec)
	}
	return out
}

// helper function to find LFNs within in agent list
func findFiles(agents map[string]string, src string) ([]AgentFiles, error) {

	// parse the input
	var lfn, block, dataset string
	if strings.Contains(src, "#") { // it is a block name, e.g. /a/b/c#123
		arr := strings.Split(src, "#")
		dataset = arr[0]
		block = arr[1]
	} else if strings.Count(src, "/") == 3 { // it is a dataset
		dataset = src
	} else { // it is lfn
		lfn = src
	}

	out := make(chan utils.ResponseType)
	defer close(out)
	umap := map[string]int{}
	for _, aurl := range agents {
		furl := fmt.Sprintf("%s/files?lfn=%s&block=%s&dataset=%s", aurl, lfn, block, dataset)
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(furl, []byte{}, out)
	}
	var agentFiles []AgentFiles
	exit := false
	for {
		select {
		case r := <-out:
			if r.Error == nil {
				var files []string
				err := json.Unmarshal(r.Data, &files)
				if err == nil {
					aurl := strings.Split(r.Url, "/files")[0]
					alias := findAlias(agents, aurl)
					if aurl != "" && alias != "" && len(files) > 0 {
						agentFiles = append(agentFiles, AgentFiles{Alias: alias, Url: aurl, Files: files})
					}
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
	return reArrange(agentFiles), nil
}

// helper function to parse source and destination parameters
func parse(agent, src, dst string) ([]model.TransferCollection, error) {
	var tc []model.TransferCollection
	var dstUrl string

	// find out list of all agents
	url := fmt.Sprintf("%s/agents", agent)
	resp := utils.FetchResponse(url, []byte{})
	if resp.Error != nil {
		return tc, resp.Error
	}
	var remoteAgents map[string]string
	e := json.Unmarshal(resp.Data, &remoteAgents)
	if e != nil {
		return tc, e
	}

	// resolve source agent name/alias and identify file to transfer
	if strings.Contains(src, ":") {
		arr := strings.Split(src, ":")
		src = arr[1]
	}

	// check if destination is ok
	dstUrl, ok := remoteAgents[dst]
	if !ok {
		log.Println("Unable to resolve destination", dst, "known agents", remoteAgents)
		return tc, fmt.Errorf("Unknown destination")
	}

	// get list of records which provide info about agent and a file
	// and construct transfer collection
	records, err := findFiles(remoteAgents, src) // src here can be either lfn/block/dataset
	if err != nil {
		return tc, err
	}
	for _, rec := range records {
		var requests []model.TransferRequest
		for _, file := range rec.Files {
			req := model.TransferRequest{SrcUrl: rec.Url, SrcAlias: rec.Alias, File: file, DstUrl: dstUrl, DstAlias: dst}
			log.Println(req.String())
			requests = append(requests, req)
		}
		tc = append(tc, model.TransferCollection{TimeStamp: time.Now().Unix(), Requests: requests})
	}
	return tc, nil
}

// Transfer client function is responsible to initiate transfer request from
// source to destination.
func Transfer(agent, src, dst string) error {

	// parse src/dst parameters and construct list of transfer requests
	collection, err := parse(agent, src, dst)
	if err != nil {
		return err
	}

	// send tranfer requests to agents concurrently via go-routine
	out := make(chan utils.ResponseType)
	defer close(out)
	umap := map[string]int{}
	for _, tc := range collection {
		furl := fmt.Sprintf("%s/request", tc.Requests[0].SrcUrl)
		d, e := json.Marshal(tc)
		if e != nil {
			return e
		}
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(furl, d, out)
	}

	// collect request responses
	exit := false
	for {
		select {
		case r := <-out:
			if r.Error != nil {
				log.Println("ERROR fail with transfer request to", r.Url)
				return r.Error
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
	return nil

}

// Agent function call agent url
func Agent(agent string) error {
	resp := utils.FetchResponse(agent, []byte{})
	log.Println(string(resp.Data))
	return resp.Error
}
