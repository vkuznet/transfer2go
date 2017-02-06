// transfer2go - Go implementation of loosely coupled, distributed agents for data transfer
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/vkuznet/transfer2go/client"
	"github.com/vkuznet/transfer2go/server"
	"github.com/vkuznet/transfer2go/utils"
)

func main() {

	// server options
	var agent string
	flag.StringVar(&agent, "agent", "", "Remote agent (registration) end-point")
	var configFile string
	flag.StringVar(&configFile, "config", "", "Agent configuration file")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbosity level")

	// client options
	var src string
	flag.StringVar(&src, "src", "", "Source end-point, either local file or AgentName:LFN")
	var dst string
	flag.StringVar(&dst, "dst", "", "Destination end-point, either AgentName or AgentName:LFN")

	// upload options
	var upload string
	flag.StringVar(&upload, "upload", "", "Meta-data of records to upload")

	flag.Parse()
	utils.CheckX509()
	utils.VERBOSE = verbose
	if configFile != "" {
		data, err := ioutil.ReadFile(configFile)
		if err != nil {
			log.Println("Unable to read", configFile, err)
			os.Exit(1)
		}
		var config server.Config
		err = json.Unmarshal(data, &config)
		if err != nil {
			log.Println("Unable to parse", configFile, err)
			os.Exit(1)
		}
		if config.Catalog == "" {
			pwd, err := os.Getwd()
			if err != nil {
				log.Println("Unable to get current directory", err)
				os.Exit(1)
			}
			config.Catalog = pwd // use current directory as catalog
		}
		if config.Workers == 0 {
			config.Workers = 10 // default value
		}
		if config.QueueSize == 0 {
			config.QueueSize = 100 // default value
		}
		if config.Protocol == "" {
			config.Protocol = "http" // default value
		}
		if config.Port == 0 {
			config.Port = 8989
		}
		if config.Register == "" {
			log.Println("WARNING this agent is not registered with remote ones, either provide register in your config or invoke register API call")
		}

		server.Server(config)
	} else {
		var err error
		if src == "" { // no transfer request
			client.Agent(agent)
		} else if upload != "" {
			err = client.Upload(agent, upload)
		} else {
			err = client.Transfer(agent, src, dst)
		}
		if err != nil {
			log.Printf("Unable to transfer %s/%s to %s", agent, src, dst)
			os.Exit(1)
		}
	}
}

// helper function to construct site name
func makeSiteName() string {
	host, err := os.Hostname()
	if err != nil {
		panic(fmt.Sprintf("Unable to get hostname, error=%v", err))
	}
	return fmt.Sprintf("T4_%s_%v", host, os.Getuid())
}
