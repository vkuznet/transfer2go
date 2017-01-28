// transfer2go/client - Go implementation transfer2go client
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package client

import (
	"fmt"
)

var VERBOSE int

func Process(agent, src, dst string) error {
	fmt.Println("### Process", agent, src, dst)
	return nil
}

func Status(agent string) error {
	resp := FetchResponse(agent+"/status", []byte{})
	fmt.Println("### Status", agent, string(resp.Data))
	return resp.Error
}
