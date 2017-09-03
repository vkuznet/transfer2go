// transfer2go - Go implementation of loosely coupled, distributed agents for data transfer
//
// Author: Valentin Kuznetsov <vkuznet@gmail.com>
//
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"time"

	log "github.com/sirupsen/logrus"
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
	var version bool
	flag.BoolVar(&version, "version", false, "Show version")

	// client options
	var src string
	flag.StringVar(&src, "src", "", "Source end-point, either local file or AgentName:LFN")
	var dst string
	flag.StringVar(&dst, "dst", "", "Destination end-point, either AgentName or AgentName:LFN")
	var register string
	flag.StringVar(&register, "register", "", "File with meta-data of records in JSON data format to register at remote agent")
	var model string
	flag.StringVar(&model, "model", "pull", "Transfer model: pull (data transfer through main agent), push (data transfer from src to dst directly)")

	var authVar bool
	flag.BoolVar(&authVar, "auth", true, "To disable the auth layer")

	flag.Parse()

	if version {
		fmt.Println(info())
		os.Exit(0)

	}
	if authVar {
		utils.CheckX509()
	}

	utils.VERBOSE = verbose
	if configFile != "" {
		data, err := ioutil.ReadFile(configFile)
		if err != nil {
			log.WithFields(log.Fields{
				"configFile": configFile,
			}).Fatal("Unable to read", err)
		}
		var config server.Config
		err = json.Unmarshal(data, &config)
		if err != nil {
			log.WithFields(log.Fields{
				"configFile": configFile,
			}).Fatal("Unable to parse", err)
		}
		if config.Catalog == "" {
			pwd, err := os.Getwd()
			if err != nil {
				log.Fatal("Unable to get current directory", err)
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
		if agent != "" {
			config.Register = agent
		}
		if config.Register == "" {
			log.Warn("WARNING this agent is not registered with remote ones, either provide register in your config or invoke register API call")
		}

		server.Init(authVar)
		server.Server(config)
	} else {
		var err error
		if register != "" {
			err = client.Register(agent, register)
		} else if src == "" { // no transfer request
			client.Agent(agent)
		} else {
			if model == "pull" {
				client.RegisterRequest(agent, src, dst)
			} else if model == "push" {
				client.Transfer(agent, src, dst)
			} else {
				log.Fatal("Unknown transfer model")
			}
		}
		if err != nil {
			log.Fatal(err)
		}
	}
}

// helper function to return current version
func info() string {
	goVersion := runtime.Version()
	tstamp := time.Now()
	return fmt.Sprintf("Build: git={{VERSION}} go=%s date=%s", goVersion, tstamp)
}

// helper function to construct site name
func makeSiteName() string {
	host, err := os.Hostname()
	if err != nil {
		panic(fmt.Sprintf("Unable to get hostname, error=%v", err))
	}
	return fmt.Sprintf("T4_%s_%v", host, os.Getuid())
}
