// transfer2go agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package server

import (
	//     "bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/vkuznet/transfer2go/client"
)

// profiler, see https://golang.org/pkg/net/http/pprof/
import _ "net/http/pprof"

// AgentInfo type
type AgentInfo struct {
	Agent string
	Alias string
}

// Catalog type
type Catalog struct {
	Type     string `json:type`
	Uri      string `json:uri`
	Login    string `json:login`
	Password string `json:password`
}

// Metrics of the agent
type Metrics struct {
	Meter        metrics.Meter
	WorkerMeters []metrics.Meter
}

// ServerMetrics defines various metrics about the agent
var ServerMetrics Metrics

// globals used in server/handlers
var _myself, _alias string
var _agents map[string]string
var _catalog Catalog

// init
func init() {
	_agents = make(map[string]string)
}

// register a new (alias, agent) pair in agent (register)
func register(register, alias, agent string) error {
	log.Printf("Register %s as %s on %s\n", agent, alias, register)
	// register myself with another agent
	params := AgentInfo{Agent: _myself, Alias: _alias}
	data, err := json.Marshal(params)
	if err != nil {
		log.Println("ERROR, unable to marshal params %v", params)
	}
	url := fmt.Sprintf("%s/register", register)
	resp := client.FetchResponse(url, data) // POST request
	return resp.Error
}

// helper function to register agent with all distributed agents
func registerAgents(aName string) {
	// now ask remote server for its list of agents and update internal map
	if aName != "" && len(aName) > 0 {
		register(aName, _alias, _myself) // submit remote registration of given agent name
		aurl := fmt.Sprintf("%s/agents", aName)
		resp := client.FetchResponse(aurl, []byte{})
		var remoteAgents map[string]string
		e := json.Unmarshal(resp.Data, &remoteAgents)
		if e == nil {
			for key, val := range remoteAgents {
				if _, ok := _agents[key]; !ok {
					_agents[key] = val // register remote agent/alias pair internally
				}
			}
		}
	}

	// complete registration with other agents
	for alias, agent := range _agents {
		if agent == aName || alias == _alias {
			continue
		}
		register(agent, _alias, _myself) // submit remote registration of given agent name
	}
}

// Server implementation
func Server(port, url, alias, aName, catalog, mfile string, minterval int64) {
	_myself = url
	_alias = alias
	arr := strings.Split(url, "/")
	base := ""
	if len(arr) > 3 {
		base = fmt.Sprintf("/%s", strings.Join(arr[3:], "/"))
	}
	log.Printf("Start agent: url=%s, port=%s, base=%s", url, port, base)

	// register self agent URI in remote agent and vice versa
	_agents[_alias] = _myself
	registerAgents(aName)

	// define catalog
	if stat, err := os.Stat(catalog); err == nil && stat.IsDir() {
		_catalog = Catalog{Type: "filesystem", Uri: catalog}
	} else {
		c, e := ioutil.ReadFile(catalog)
		if e != nil {
			log.Fatalf("Unable to read catalog file, error=%v\n", err)
		}
		err := json.Unmarshal([]byte(c), &_catalog)
		if err != nil {
			log.Fatalf("Unable to parse catalog JSON file, error=%v\n", err)
		}
	}
	log.Println("Catalog", _catalog)

	// define handlers
	http.HandleFunc(fmt.Sprintf("%s/status", base), StatusHandler)
	http.HandleFunc(fmt.Sprintf("%s/agents", base), AgentsHandler)
	http.HandleFunc(fmt.Sprintf("%s/register", base), RegisterHandler)
	http.HandleFunc(fmt.Sprintf("%s/", base), RequestHandler)

	// register metrics
	f, e := os.OpenFile(mfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		log.Fatalf("error opening file: %v", e)
	}
	defer f.Close()

	r := metrics.DefaultRegistry
	m := metrics.GetOrRegisterMeter("requests", r)
	go metrics.Log(r, time.Duration(minterval)*time.Second, log.New(f, "metrics: ", log.Lmicroseconds))

	// start dispatcher for incoming requests
	var workerMeters []metrics.Meter
	var maxWorker, maxQueue int
	var err error
	maxWorker, err = strconv.Atoi(os.Getenv("MAX_WORKERS"))
	if err != nil {
		maxWorker = 10
	}
	maxQueue, err = strconv.Atoi(os.Getenv("MAX_QUEUE"))
	if err != nil {
		maxQueue = 100
	}

	for i := 0; i < maxWorker; i++ {
		wm := metrics.GetOrRegisterMeter(fmt.Sprintf("worker_%d", i), r)
		workerMeters = append(workerMeters, wm)
	}
	ServerMetrics = Metrics{Meter: m, WorkerMeters: workerMeters}

	dispatcher := NewDispatcher(maxWorker, maxQueue)
	dispatcher.Run()
	log.Println("Start dispatcher with", maxWorker, "workers, queue size", maxQueue)

	// start server
	err = http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
