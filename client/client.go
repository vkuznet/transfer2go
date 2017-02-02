package client

// transfer2go/client - Go implementation transfer2go client
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/vkuznet/transfer2go/model"
	"github.com/vkuznet/transfer2go/utils"
)

// Transfer client function is responsible to initiate transfer request from
// source to destination.
func Transfer(agent, src, dst string) error {
	var transfer, upload bool
	var srcFile, srcAlias, srcUrl, dstFile, dstAlias, dstUrl string
	if stat, err := os.Stat(src); err == nil && !stat.IsDir() {
		// local file, we need to transfer it to the destination
		upload = true
		srcFile = src
		srcUrl = agent
	}
	if strings.Contains(src, ":") {
		arr := strings.Split(src, ":")
		srcAlias = arr[0]
		srcFile = arr[1]
	}
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
		return resp.Error
	}
	var remoteAgents map[string]string
	e := json.Unmarshal(resp.Data, &remoteAgents)
	if e != nil {
		return e
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
		fmt.Println("Unable to resolve destination", dst)
		fmt.Println("Map of known agents", remoteAgents)
		return fmt.Errorf("Unknown destination")
	}

	if transfer {
		fmt.Println("### Transfer", srcAlias, srcUrl, srcFile, "=>", dstAlias, dstUrl, dstFile)

		// Read data from source agent
		url = fmt.Sprintf("%s/files?pattern=%s", srcUrl, srcFile)
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
			requests = append(requests, model.TransferRequest{SrcUrl: srcUrl, SrcAlias: srcAlias, DstUrl: dstUrl, DstAlias: dst, File: fname, TimeStamp: ts})
		}
		ts := time.Now().Unix()
		transferCollection := model.TransferCollection{TimeStamp: ts, Requests: requests}

		url = fmt.Sprintf("%s/request", srcUrl)
		d, e := json.Marshal(transferCollection)
		if e != nil {
			return e
		}
		resp = utils.FetchResponse(url, d)
		return resp.Error
	}

	if upload {
		fmt.Println("### Upload", src, "to site", dstAlias, dstUrl, "as", dstFile)
		data, err := ioutil.ReadFile(srcFile)
		if err != nil {
			return err
		}
		hash, bytes := utils.Hash(data)
		d := "/a/b/c" // dataset name
		b := "123"    // block name
		transferData := model.TransferData{File: dstFile, Dataset: d, Block: b, SrcUrl: srcUrl, SrcAlias: srcAlias, DstUrl: dstUrl, DstAlias: dstAlias, Data: data, Hash: hash, Bytes: bytes}
		url = fmt.Sprintf("%s/transfer", dstUrl)
		data, err = json.Marshal(transferData)
		if err != nil {
			return err
		}
		resp = utils.FetchResponse(url, data)
		return resp.Error
	}

	return fmt.Errorf("Unable to understand client request, src=%v to dst=%v", src, dst)
}

// Status function provides status about given agent
func Status(agent string) error {
	resp := utils.FetchResponse(agent+"/status", []byte{})
	fmt.Println("### Status", agent, string(resp.Data))
	return resp.Error
}
