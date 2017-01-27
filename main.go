// transfer2go - Go implementation of loosely coupled, distributed agents for data transfer
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package main

import (
	"flag"
	"fmt"
	"github.com/vkuznet/transfer2go/client"
	"github.com/vkuznet/transfer2go/server"
	"os"
	"os/user"
)

func main() {
	var agent string
	flag.StringVar(&agent, "agent", "", "Remote agent end-point")
	var status bool
	flag.BoolVar(&status, "status", false, "Return status info about the agent")
	var src string
	flag.StringVar(&src, "src", "", "Source logical file name")
	var dst string
	flag.StringVar(&dst, "dst", "", "Destination end-point")
	var register string
	flag.StringVar(&register, "register", "", "Registration end-point")
	var uri string
	flag.StringVar(&uri, "uri", "", "Server end-point URI, e.g. localhost:8989")
	var interval int64
	flag.Int64Var(&interval, "interval", 60, "Server metrics interval, default 60 seconds")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbosity level, default 0")
	flag.Parse()
	checkX509()
	if uri != "" {
		server.Server(uri, register, interval)
	} else {
		client.VERBOSE = verbose
		if status {
			client.Status(agent)
		} else {
			client.Process(agent, src, dst)
		}
	}
}

// helper function to check X509 settings
func checkX509() {
	uproxy := os.Getenv("X509_USER_PROXY")
	uckey := os.Getenv("X509_USER_KEY")
	ucert := os.Getenv("X509_USER_CERT")
	var check int
	if uproxy == "" {
		// check if /tmp/x509up_u$UID exists
		u, err := user.Current()
		if err == nil {
			fname := fmt.Sprintf("/tmp/x509up_u%s", u.Uid)
			if _, err := os.Stat(fname); err != nil {
				check += 1
			}
		}
	}
	if uckey == "" && ucert == "" {
		check += 1
	}
	if check > 1 {
		fmt.Println("Neither X509_USER_PROXY or X509_USER_KEY/X509_USER_CERT are set")
		fmt.Println("In order to run please obtain valid proxy via \"voms-proxy-init -voms cms -rfc\"")
		fmt.Println("and setup X509_USER_PROXY or setup X509_USER_KEY/X509_USER_CERT in your environment")
		os.Exit(-1)
	}
}
