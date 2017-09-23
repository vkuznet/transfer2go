package client

// transfer2go/client - Go implementation transfer2go client
//
// Author: Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/adler32"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vkuznet/transfer2go/core"
	"github.com/vkuznet/transfer2go/utils"
)

// ActionRequest provides structure submitted by clients to perform certain action on main agent
type ActionRequest struct {
	Delay    int    `json:"delay"`    // transfer delay time, i.e. post-pone transfer
	Id       string `json:"id"`       // unique id of each request
	Priority int    `json:"priority"` // priority of request
	Action   string `json:"action"`   // which action to apply
}

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

// helper function to find remote agents
func findAgents(agent string) map[string]string {

	// find out list of all agents
	url := fmt.Sprintf("%s/agents", agent)
	resp := utils.FetchResponse(url, []byte{})
	if resp.Error != nil {
		log.WithFields(log.Fields{
			"Agent": agent,
		}).Error("Unable to get list of agents")
	}
	var remoteAgents map[string]string
	e := json.Unmarshal(resp.Data, &remoteAgents)
	if e != nil {
		log.WithFields(log.Fields{
			"Agent": agent,
		}).Error("Unable to unmarshal response from agent")
	}
	return remoteAgents
}

// Agent function call agent url
func Agent(agent string) error {
	resp := utils.FetchResponse(agent, []byte{})
	log.Println(string(resp.Data))
	return resp.Error
}

// helper function to parse source and destination parameters
// here anent represent registration agent url while src and dst may be
// in a form of alias:data or url:data
func parseRequest(agent, src, dst string) (core.TransferRequest, error) {
	var req core.TransferRequest
	var agentAlias, srcUrl, srcAlias, dstUrl, dstAlias string

	// resolve source agent name/alias and identify file to transfer
	var data string
	if strings.Contains(src, ":") {
		arr := strings.Split(src, ":")
		src = arr[0]
		data = arr[1]
	}
	if len(data) == 0 {
		log.WithFields(log.Fields{
			"Source":      src,
			"Destination": dst,
		}).Error("Unable to resolve destination")
		return req, fmt.Errorf("No data is specified to transfer")
	}
	// resolve all information about agents
	remoteAgents := findAgents(agent)
	for k, rurl := range remoteAgents {
		if rurl == agent {
			agentAlias = k
		}
		if k == src || rurl == src {
			srcUrl = rurl
			srcAlias = k
		}
		if k == dst || rurl == dst {
			dstUrl = rurl
			dstAlias = k
		}
	}
	if agentAlias == "" || srcAlias == "" || srcUrl == "" || dstAlias == "" || dstUrl == "" {
		log.WithFields(log.Fields{
			"Agent":       agent,
			"Source":      src,
			"Destination": dst,
			"AgentAlias":  agentAlias,
			"SrcAlias":    srcAlias,
			"SrcUrl":      srcUrl,
			"DstAlias":    dstAlias,
			"DstUrl":      dstUrl,
		}).Error("Unable to resolve agent urls|names")
		return req, fmt.Errorf("Name resolution problem")
	}
	req = core.TransferRequest{RegUrl: agent, RegAlias: agentAlias, SrcUrl: srcUrl, SrcAlias: srcAlias, DstUrl: dstUrl, DstAlias: dstAlias}
	if strings.Contains(data, "#") { // it is a block name, e.g. /a/b/c#123
		req.Block = data
	} else if strings.Count(data, "/") == 3 { // it is a dataset
		req.Dataset = data
	} else { // it is lfn
		req.File = data
	}
	log.Info(req.String())
	return req, nil
}

// helper function to read a file from given path, it calculates file hash during this process
func readFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	var bytes int64
	// Create a synchronous in-memory pipe. This pipe will
	// allow us to use a Writer as a Reader.
	pipeReader, pipeWriter := io.Pipe()
	// create a hasher to calculate data hash
	hasher := adler32.New()
	// Wait for the reading and writing processes of the io.pipe
	done := make(chan error)
	// Create a goroutine to write chunk wise in io.pipe
	go func() {
		defer pipeWriter.Close()
		_, err = io.Copy(pipeWriter, file)
		if err != nil {
			done <- err
			return
		}
		done <- nil
	}()
	// Create a goroutine to read chunk wise from io.pipe
	go func() {
		defer pipeReader.Close()
		bytes, err = io.Copy(hasher, pipeReader)
		if err != nil {
			done <- err
			return
		}
		done <- nil
	}()
	err = <-done
	if err != nil {
		return "", 0, err
	}
	err = <-done
	if err != nil {
		return "", 0, err
	}
	hash := hex.EncodeToString(hasher.Sum(nil))
	return hash, bytes, nil
}

// helper function to submit tranfer requests to given url
func submitRequest(furl string, requests []core.TransferRequest) {
	d, err := json.Marshal(requests)
	if err != nil {
		log.WithFields(log.Fields{
			"Url":   furl,
			"Error": err,
		}).Info("Unable to marshal request")
		return
	}
	resp := utils.FetchResponse(furl, d)
	if resp.Error != nil || resp.StatusCode != 200 {
		log.WithFields(log.Fields{
			"Url":   furl,
			"Error": resp.Error,
		}).Info("Error while registering request to main agent")
		return
	}
	log.WithFields(log.Fields{
		"Url": furl,
	}).Info("successfully registered request")
}

// Transfer performs transfer data from source to destination (PUSH model)
func Transfer(agent, src, dst string) {
	req, err := parseRequest(agent, src, dst)
	if err != nil {
		log.WithFields(log.Fields{
			"Agent":       agent,
			"Source":      src,
			"Destination": dst,
			"Error":       err,
		}).Info("Unable to parse request")
	}
	var requests []core.TransferRequest
	requests = append(requests, req)
	// make request to source url to transfer data
	furl := fmt.Sprintf("%s/request", req.SrcUrl)
	submitRequest(furl, requests)
}

// RegisterRequest performs registration of transfer request in given agent (PULL mode)
func RegisterRequest(agent string, src string, dst string) {
	var requests []core.TransferRequest
	furl := agent + "/request"
	req, err := parseRequest(agent, src, dst)
	if err != nil {
		log.WithFields(log.Fields{
			"Agent":       agent,
			"Source":      src,
			"Destination": dst,
			"Error":       err,
		}).Info("Unable to parse request")
		return
	}
	requests = append(requests, req)
	submitRequest(furl, requests)
}

// Register function upload given meta-data to the agent and register them in its TFC
func Register(agent, fname string) {
	// read inpuf file name which contains records meta-data (catalog entries)
	c, e := ioutil.ReadFile(fname)
	if e != nil {
		log.WithFields(log.Fields{
			"Agent": agent,
			"File":  fname,
			"Error": e,
		}).Error("Unable to read the file")
		return
	}
	var uploadRecords, records []core.CatalogEntry
	err := json.Unmarshal([]byte(c), &records)
	if err != nil {
		log.WithFields(log.Fields{
			"Agent": agent,
			"File":  fname,
			"Error": err,
		}).Error("Unable to parse catalog JSON file")
		return
	}
	// TODO: so far we scan every record and read a file to get its hash
	// this work only for local filesystem, but I don't know how it will work
	// for remote storage
	for _, rec := range records {
		if rec.Lfn == "" || rec.Pfn == "" || rec.Block == "" || rec.Dataset == "" {
			e := fmt.Errorf("Record must have at least the following fields: lfn, pfn, block, dataset, instead received: %v\n", rec)
			log.WithFields(log.Fields{
				"Agent": agent,
				"File":  fname,
				"Error": e,
			}).Error("No input data provided with record")
			return
		}
		hash, bytes, err := readFile(rec.Pfn)
		if err != nil {
			log.WithFields(log.Fields{
				"Agent": agent,
				"File":  fname,
				"Error": err,
			}).Error("Unable to read rec.Pfn")
			return
		}
		r := core.CatalogEntry{Lfn: rec.Lfn, Pfn: rec.Pfn, Block: rec.Block, Dataset: rec.Dataset, Hash: hash, Bytes: bytes}
		uploadRecords = append(uploadRecords, r)
	}
	d, e := json.Marshal(uploadRecords)
	if e != nil {
		log.WithFields(log.Fields{
			"Agent": agent,
			"File":  fname,
			"Error": e,
		}).Error("Unable to Marshal upload records")
		return
	}
	url := fmt.Sprintf("%s/tfc", agent)
	resp := utils.FetchResponse(url, d)
	if resp.Error != nil {
		e := fmt.Errorf("Unable to upload, url=%s, data=%s, err=%v\n", url, string(resp.Data), resp.Error)
		log.WithFields(log.Fields{
			"Agent": agent,
			"File":  fname,
			"Error": e,
		}).Error("Unable to fetch response")
		return
	}
	log.WithFields(log.Fields{
		"Agent": agent,
		"Size":  len(uploadRecords),
	}).Info("Registered records in")
}

// ProcessAction performs request approval in given agent
func ProcessAction(agent, jsonString string) {
	var req ActionRequest
	err := json.Unmarshal([]byte(jsonString), &req)
	if err != nil {
		log.WithFields(log.Fields{
			"JSON":  jsonString,
			"Error": err,
		}).Error("Error unable to unmarshal input json string")
		return
	}
	rid := req.Id
	if rid == "" {
		log.WithFields(log.Fields{
			"JSON":  jsonString,
			"Error": err,
		}).Error("unknown request Id")
		return
	}
	if req.Action != "approve" && req.Action != "delete" {
		log.WithFields(log.Fields{
			"JSON":   jsonString,
			"Action": req.Action,
		}).Error("unknown action")
		return
	}
	furl := fmt.Sprintf("%s/action", agent)
	var jobs []core.Job
	r := core.TransferRequest{Id: rid}
	job := core.Job{TransferRequest: r, Action: req.Action}
	jobs = append(jobs, job)
	d, e := json.Marshal(jobs)
	if e != nil {
		log.WithFields(log.Fields{
			"Url":   furl,
			"Id":    rid,
			"Job":   job,
			"Error": e,
		}).Error("Error while marshal job transfer request")
		return
	}
	resp := utils.FetchResponse(furl, d)
	if resp.Error != nil || resp.StatusCode != 200 {
		log.WithFields(log.Fields{
			"Url":   furl,
			"Id":    rid,
			"Error": resp.Error,
		}).Error("Error while fetching response from main agent")
		return
	}
	log.WithFields(log.Fields{
		"Url":    furl,
		"Id":     rid,
		"Action": req.Action,
	}).Info("successfully process the action")
}

// ShowRequests list request of a given type from an agent
func ShowRequests(agent, rtype string) {
	furl := fmt.Sprintf("%s/list?type=%s", agent, rtype)
	var args []byte
	resp := utils.FetchResponse(furl, args)
	if resp.Error != nil || resp.StatusCode != 200 {
		log.WithFields(log.Fields{
			"Url":   furl,
			"Type":  rtype,
			"Error": resp.Error,
		}).Error("Error while fetching list of request from the agent")
		return
	}
	var requests []core.TransferRequest
	err := json.Unmarshal(resp.Data, &requests)
	if err != nil {
		log.WithFields(log.Fields{
			"Url":   furl,
			"Type":  rtype,
			"Error": resp.Error,
		}).Error("Error during unmarshalling HTTP response")
		return
	}
	for _, r := range requests {
		log.Info(r.String())
	}
}
