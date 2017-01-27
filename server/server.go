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
)

// profiler, see https://golang.org/pkg/net/http/pprof/
import _ "net/http/pprof"

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
	msg := fmt.Sprintf("Status content: %v", time.Now())
	w.Write([]byte(msg))
	w.WriteHeader(http.StatusOK)
}

// Server implementation
func Server(agent, register string, interval int64) {
	log.Printf("Start agent %s", agent)
	arr := strings.Split(agent, ":")
	base := arr[0]
	port := arr[1]
	if base != "" {
		base = fmt.Sprintf("/%s", base)
	}
	http.HandleFunc(fmt.Sprintf("%s/status", base), StatusHandler)
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
