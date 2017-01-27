// transfer2go agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package server

import (
	"fmt"
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

// globals
var _myself string
var _agents []string

// Metrics of the agent
type Metrics struct {
	Meter        metrics.Meter
	WorkerMeters []metrics.Meter
}

// ServerMetrics defines various metrics about the agent
var ServerMetrics Metrics

// StatusHandler provides information about the agent
func StatusHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	msg := fmt.Sprintf("Status content: %v\nagents: %v\n", time.Now(), _agents)
	w.Write([]byte(msg))
}

// register a new agent
func register(agent string) error {
	log.Printf("Register %s\n", agent)
	// add given agent to internal list of agents
	_agents = append(_agents, agent)
	// register myself with another agent
	url := fmt.Sprintf("%s/register?agent=%s", agent, _myself)
	resp := client.FetchResponse(url, "")
	return resp.Error
}

// RegisterHandler registers current agent with another one
func RegisterHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	uri := r.FormValue("agent")  // read agent parameter value
	err := register(string(uri)) // perform agent registration
	if err != nil {
		// the order is important we first need to write header and then (!!!) the body
		// see http://stackoverflow.com/questions/27972715/multiple-response-writeheader-calls-in-really-simple-example
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Unable to regiser uri"))
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Server implementation
func Server(agent, aName string, interval int64) {
	log.Printf("Start agent %s", agent)
	arr := strings.Split(agent, ":")
	base := arr[0]
	port := arr[1]
	if base != "" {
		base = fmt.Sprintf("/%s", base)
	}
	hostname, e := os.Hostname()
	if e != nil {
		log.Fatalf("Unable to get hostname, error=%v\n", e)
	}
	_myself = fmt.Sprintf("http://%s%s:%s", hostname, base, port)
	if aName != "" {
		register(aName) // submit remote registration of given agent name
	}
	http.HandleFunc(fmt.Sprintf("%s/status", base), StatusHandler)
	http.HandleFunc(fmt.Sprintf("%s/register", base), RegisterHandler)
	http.HandleFunc(fmt.Sprintf("%s/", base), RequestHandler)

	// register metrics
	r := metrics.DefaultRegistry
	m := metrics.GetOrRegisterMeter("requests", r)
	go metrics.Log(r, time.Duration(interval)*time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

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
