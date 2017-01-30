// transfer2go/client - Go implementation transfer2go client
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

var VERBOSE int

// TransferData struct holds all attributes of transfering data, such as name, checksum, data, etc.
type TransferData struct {
	Name   string `json:name`
	Data   []byte `json:data`
	Chksum string `json:chksum`
}

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
	if e == nil {
		// check if destination is ok
		if _, ok := remoteAgents[dst]; !ok {
			fmt.Println("Unable to resolve destination", dst)
			fmt.Println("Map of known agents", remoteAgents)
			return fmt.Errorf("Unknown destination")
		}

		for alias, aurl := range remoteAgents {
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
			fmt.Println("### found", alias, aurl, files)
			for _, fname := range files {
				data, err := ioutil.ReadFile(fname)
				if err == nil {
					chksum := "some_check_sum"
					transferData := TransferData{Name: fname, Data: data, Chksum: chksum}
					data2transfer = append(data2transfer, transferData)
				}
			}
			fmt.Println("### agent", alias, aurl, "transfer", len(data2transfer), "files")
			url = fmt.Sprintf("%s/transfer", aurl)
			d, e := json.Marshal(data2transfer)
			if e != nil {
				return e
			}
			resp := FetchResponse(url, d)
			return resp.Error
		}
	} else {
		return fmt.Errorf("Unable to resolve list of remote agents")
	}
	return nil
}

func Status(agent string) error {
	resp := FetchResponse(agent+"/status", []byte{})
	fmt.Println("### Status", agent, string(resp.Data))
	return resp.Error
}
