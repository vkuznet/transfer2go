package server

// transfer2go agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	// loads sqlite3 database layer
	_ "github.com/mattn/go-sqlite3"
	"github.com/vkuznet/transfer2go/client"
	"github.com/vkuznet/transfer2go/model"

	// web profiler, see https://golang.org/pkg/net/http/pprof
	_ "net/http/pprof"
)

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

func filePath(idir, fname string) string {
	return fmt.Sprintf("%s/%s", idir, fname)
}

// Files method of catalog returns list of files known in catalog
// TODO: implement sqlitedb catalog logic, e.g. we need to make
// a transfer and then record in DB catalog file's hash and transfer details
func (c *Catalog) Files(pattern string) []string {
	var files []string
	if c.Type == "filesystem" {
		filesInfo, err := ioutil.ReadDir(c.Uri)
		if err != nil {
			log.Println("ERROR: unable to list files in catalog", c.Uri, err)
			return []string{}
		}
		for _, f := range filesInfo {
			if pattern != "" {
				if strings.Contains(f.Name(), pattern) {
					files = append(files, filePath(c.Uri, f.Name()))
				}
			} else {
				files = append(files, filePath(c.Uri, f.Name()))
			}
		}
		return files
	} else if c.Type == "sqlitedb" {
		db, err := sql.Open(c.Type, c.Uri)
		defer db.Close()
		if err != nil {
			log.Println("ERROR: unable to list files in catalog", c.Uri, err)
			return []string{}
		}
		db.SetMaxOpenConns(100)
		db.SetMaxIdleConns(100)
		return files
	}
	return files
}

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
	http.HandleFunc(fmt.Sprintf("%s/files", base), FilesHandler)
	http.HandleFunc(fmt.Sprintf("%s/transfer", base), TransferClientBasedHandler)
	http.HandleFunc(fmt.Sprintf("%s/", base), RequestHandler)

	// define size of workers and queue, I may read it from input parameters later
	var maxWorkers, maxQueue int
	var err error
	maxWorkers, err = strconv.Atoi(os.Getenv("MAX_WORKERS"))
	if err != nil {
		maxWorkers = 10
	}
	maxQueue, err = strconv.Atoi(os.Getenv("MAX_QUEUE"))
	if err != nil {
		maxQueue = 100
	}
	dispatcher := model.NewDispatcher(maxWorkers, maxQueue, mfile, minterval)
	dispatcher.Run()
	log.Println("Start dispatcher with", maxWorkers, "workers, queue size", maxQueue)

	// start server
	err = http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
