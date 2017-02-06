package common

import "fmt"

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
	ToolOpts        string            `json:"toolopts"` // options for backend tool
	Agents          map[string]string `json:"agents"`   // list of known agents
	Addrs           []string          `json:"addrs"`    // list of all IP addresses
}

// String provides string representation of given agent status
func (a *AgentStatus) String() string {
	return fmt.Sprintf("<Agent name=%s url=%s catalog=%s protocol=%s backend=%s tool=%s toolOpts=%s transfers=%d agents=%v addrs=%v>", a.Name, a.Url, a.Catalog, a.Protocol, a.Backend, a.Tool, a.ToolOpts, a.TransferCounter, a.Agents, a.Addrs)
}
