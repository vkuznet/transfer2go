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
	"os/user"

	"github.com/vkuznet/transfer2go/client"
	"github.com/vkuznet/transfer2go/server"
	"github.com/vkuznet/transfer2go/utils"
)

func main() {

	// server options
	var port string
	flag.StringVar(&port, "port", "8989", "server port number")
	var agent string
	flag.StringVar(&agent, "agent", "", "Remote agent registration end-point")
	var configFile string
	flag.StringVar(&configFile, "config", "", "Agent configuration file")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbosity level")

	// client options
	var status bool
	flag.BoolVar(&status, "status", false, "Return status info about the agent")
	var src string
	flag.StringVar(&src, "src", "", "Source end-point, either local file or AgentName:LFN")
	var dst string
	flag.StringVar(&dst, "dst", "", "Destination end-point, either AgentName or AgentName:LFN")
	var register string
	flag.StringVar(&register, "register", "", "Registration end-point")

	flag.Parse()
	checkX509()
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

		server.Server(port, config, register)
	} else {

		if status {
			client.Status(agent)
		} else {
			err := client.Transfer(agent, src, dst)
			if err != nil {
				log.Printf("Unable to transfer %s/%s to %s", agent, src, dst)
				os.Exit(1)
			}
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
		msg := fmt.Sprintf("Neither X509_USER_PROXY or X509_USER_KEY/X509_USER_CERT are set. ")
		msg += "In order to run please obtain valid proxy via \"voms-proxy-init -voms cms -rfc\""
		msg += "and setup X509_USER_PROXY or setup X509_USER_KEY/X509_USER_CERT in your environment"
		log.Println(msg)
		os.Exit(-1)
	}
}
