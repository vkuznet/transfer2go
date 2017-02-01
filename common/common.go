package common

// transfer2go agent common structures
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

// AgentStatus data type
type AgentStatus struct {
	Url             string            `json:"url"`      // agent url
	Name            string            `json:"name"`     // agent name or alias
	TimeStamp       int64             `json:"ts"`       // time stamp
	TransferCounter int32             `json:"tc"`       // number of transfers at a given time
	Catalog         string            `json:"catalog"`  // underlying TFC catalog
	Protocol        string            `json:"protocol"` // underlying transfer protocol
	Backend         string            `json:"backend"`  // underlying transfer backend
	Tool            string            `json:"tool"`     // underlying transfer tool, e.g. xrdcp
	Agents          map[string]string `json:"agents"`   // list of known agents
	Addrs           []string          `json:"addrs"`    // list of all IP addresses
}
