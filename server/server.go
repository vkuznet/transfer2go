package server

// transfer2go agent server implementation
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/vkuznet/transfer2go/core"
	"github.com/vkuznet/transfer2go/utils"

	// web profiler, see https://golang.org/pkg/net/http/pprof
	_ "net/http/pprof"
)

// Config type holds server configuration
type Config struct {
	Name      string `json:"name"`      // agent name, aka site name
	Url       string `json:"url"`       // agent url
	Catalog   string `json:"catalog"`   // catalog file name, e.g. catalog.db
	Protocol  string `json:"protocol"`  // backend protocol, e.g. srmv2
	Backend   string `json:"backend"`   // backend, e.g. srm
	Tool      string `json:"tool"`      // backend tool, e.g. srmcp
	ToolOpts  string `json:"toolopts"`  // options for backend tool
	Mfile     string `json:"mfile"`     // metrics file name
	Minterval int64  `json:"minterval"` // metrics interval
	Staticdir string `json:"staticdir"` // static dir defines location of static files, e.g. sql,js templates
	Workers   int    `json:"workers"`   // number of workers
	QueueSize int    `json:"queuesize"` // total size of the queue
	Port      int    `json:"port"`      // port number given server runs on, default 8989
	Base      string `json:"base"`      // URL base path for agent server, it will be extracted from Url
	Register  string `json:"register"`  // remote agent URL to register
	ServerKey string `json:"serverkey"` // server key file
	ServerCrt string `json:"servercrt"` // server crt file
}

// String returns string representation of Config data type
func (c *Config) String() string {
	return fmt.Sprintf("<Config: name=%s url=%s port=%d base=%s catalog=%s protocol=%s backend=%s tool=%s opts=%s mfile=%s minterval=%d staticdir=%s workders=%d queuesize=%d register=%s>", c.Name, c.Url, c.Port, c.Base, c.Catalog, c.Protocol, c.Backend, c.Tool, c.ToolOpts, c.Mfile, c.Minterval, c.Staticdir, c.Workers, c.QueueSize, c.Register)
}

// AgentInfo data type
type AgentInfo struct {
	Agent string
	Alias string
}

// AgentProtocol data type
type AgentProtocol struct {
	Protocol string `json:"protocol"` // protocol name, e.g. srmv2
	Backend  string `json:"backend"`  // backend storage end-point, e.g. srm://cms-srm.cern.ch:8443/srm/managerv2?SFN=
	Tool     string `json:"tool"`     // actual executable, e.g. /usr/local/bin/srmcp
	ToolOpts string `json:"toolopts"` // options for backend tool
}

// globals used in server/handlers
var _myself, _alias, _protocol, _backend, _tool, _toolOpts string
var _agents map[string]string
var _config Config

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
		log.Println("ERROR, unable to marshal params", params)
	}
	url := fmt.Sprintf("%s/register", register)
	resp := utils.FetchResponse(url, data) // POST request
	// check return status code
	if resp.StatusCode != 200 {
		return fmt.Errorf("Response %s, error=%s", resp.Status, string(resp.Data))
	}
	return resp.Error
}

// helper function to register agent with all distributed agents
func registerAtAgents(aName string) {
	// register itself
	if _, ok := _agents[_alias]; ok {
		log.Fatal("ERROR unable to register", _alias, "at", _agents, "since this name already exists")
	}
	_agents[_alias] = _myself

	// now ask remote server for its list of agents and update internal map
	if aName != "" && len(aName) > 0 {
		err := register(aName, _alias, _myself) // submit remote registration of given agent name
		if err != nil {
			log.Fatalf("ERROR Unable to register: %s %s %s %s %v", _alias, _myself, "at", aName, err)
		}
		aurl := fmt.Sprintf("%s/agents", aName)
		resp := utils.FetchResponse(aurl, []byte{})
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
func Server(config Config) {
	_config = config
	_myself = config.Url
	_alias = config.Name
	_protocol = config.Protocol
	_backend = config.Backend
	_tool = config.Tool
	_toolOpts = config.ToolOpts
	utils.STATICDIR = config.Staticdir
	arr := strings.Split(_myself, "/")
	base := ""
	if len(arr) > 3 {
		base = fmt.Sprintf("/%s", strings.Join(arr[3:], "/"))
	}
	port := "8989" // default port, the port here is a string type since we'll use it later in http.ListenAndServe
	if config.Port != 0 {
		port = fmt.Sprintf("%d", config.Port)
	}
	config.Base = base
	log.Println("Agent", config.String())

	// register self agent URI in remote agent and vice versa
	registerAtAgents(config.Register)

	// define catalog
	c, e := ioutil.ReadFile(config.Catalog)
	if e != nil {
		log.Fatalf("Unable to read catalog file, error=%v\n", e)
	}
	err := json.Unmarshal([]byte(c), &core.TFC)
	if err != nil {
		log.Fatalf("Unable to parse catalog JSON file, error=%v\n", err)
	}
	// open up Catalog DB
	dbtype := core.TFC.Type
	dburi := core.TFC.Uri // TODO: may be I need to change this based on DB Login/Password, check MySQL
	dbowner := core.TFC.Owner
	db, dberr := sql.Open(dbtype, dburi)
	defer db.Close()
	if dberr != nil {
		log.Fatalf("ERROR sql.Open, %v\n", dberr)
	}
	dberr = db.Ping()
	if dberr != nil {
		log.Fatalf("ERROR db.Ping, %v\n", dberr)
	}

	core.DB = db
	core.DBTYPE = dbtype
	core.DBSQL = core.LoadSQL(dbtype, dbowner)
	log.Println("Catalog", core.TFC)

	// define handlers
	http.HandleFunc(fmt.Sprintf("%s/", base), AuthHandler)

	// initialize task dispatcher
	dispatcher := core.NewDispatcher(config.Workers, config.QueueSize, config.Mfile, config.Minterval)
	dispatcher.Run()
	log.Println("Start dispatcher with", config.Workers, "workers, queue size", config.QueueSize)

	if authVar {
		//start HTTPS server which require user certificates
		server := &http.Server{
			Addr: ":" + port,
			TLSConfig: &tls.Config{
				ClientAuth: tls.RequestClientCert,
			},
		}
		err = server.ListenAndServeTLS(config.ServerCrt, config.ServerKey)
	} else {
		err = http.ListenAndServe(":"+port, nil)   // Start server without user certificates
	}

	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
