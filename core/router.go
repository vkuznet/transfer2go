package core

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"

	"github.com/robfig/cron"
	"github.com/sajari/regression"
	log "github.com/sirupsen/logrus"
	"github.com/vkuznet/transfer2go/utils"
	"gopkg.in/fatih/set.v0"
)

// If error occures while getting predictions set default value to MinFloat
const MIN_FLOAT = -1000000

type Router struct {
	CronInterval     string                 // Helps to set hourly based cron job
	LinearRegression *regression.Regression // machine learning model
	CSVfile          string                 // historical data file
	Agents           *map[string]string     // list of connected agents
}

// struct to store source informations
type SourceStats struct {
	SrcUrl     string
	SrcAlias   string
	catalogSet *set.SetNonTS
	prediction float64
	Requests   []TransferRequest
}

// AgentRouter helps to call router's methods
var AgentRouter Router

// newRouter returns new instance of Router type
func NewRouter(interval string, agent *map[string]string, csvFile string) *cron.Cron {
	lr := new(regression.Regression)
	lr.SetObserved("Get throughput")
	lr.SetVar(0, "CPU usage")
	lr.SetVar(1, "Memory usage")

	timeConfig := "@every " + interval // It works on this format - http://golang.org/pkg/time/#ParseDuration
	c := cron.New()
	c.AddFunc(timeConfig, train)
	AgentRouter = Router{CronInterval: interval, Agents: agent, LinearRegression: lr, CSVfile: csvFile}
	return c
}

// Function to train the agent
func train() {
	var dataPoints []TransferData
	for _, source := range *AgentRouter.Agents {
		data, err := getHistory(source)
		if err == nil {
			dataPoints = append(dataPoints, data...)
		}
	}
	if len(dataPoints) == 0 {
		return
	}

	// Reinitialize the model
	lr := new(regression.Regression)
	lr.SetObserved("Get throughput")
	lr.SetVar(0, "CPU usage")
	lr.SetVar(1, "Memory usage")
	AgentRouter.LinearRegression = lr

	for _, obj := range dataPoints {
		AgentRouter.LinearRegression.Train(regression.DataPoint(obj.Throughput, []float64{obj.CpuUsage, obj.MemUsage}))
	}
	AgentRouter.LinearRegression.Run()
	log.WithFields(log.Fields{
		"CronInterval": AgentRouter.CronInterval,
		"Data":         AgentRouter.CSVfile,
	}).Println("Router successfully retrained")

	err := convertToCSV(dataPoints)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Println("Error while saving data")
	}
}

// Convert struct to csv format
func convertToCSV(dataPoints []TransferData) error {
	file, err := os.Create(AgentRouter.CSVfile)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, obj := range dataPoints {
		str := []string{strconv.FormatFloat(obj.CpuUsage, 'f', -1, 64), strconv.FormatFloat(obj.MemUsage, 'f', -1, 64), strconv.FormatFloat(obj.Throughput, 'f', -1, 64)}
		writer.Write(str)
	}
	return nil
}

// function to get historical data of agent
func getHistory(source string) ([]TransferData, error) {
	url := fmt.Sprintf("%s/history?duration=%s", source, AgentRouter.CronInterval)
	resp := utils.FetchResponse(url, []byte{})
	if resp.Error != nil {
		return nil, resp.Error
	}
	var transferRecords []TransferData
	err := json.Unmarshal(resp.Data, &transferRecords)
	if err != nil {
		return nil, err
	}
	return transferRecords, nil
}

// function to train router by previous data(After restarting it)
func (r *Router) InitialTrain() {
	// Check if router has previous data, if not run train method for the first time
	if _, err := os.Stat(r.CSVfile); !os.IsNotExist(err) {
		trainingData, err := readCSVfile(r.CSVfile)
		if err != nil {
			log.WithFields(log.Fields{
				"Error": err,
			}).Println("Error while training router")
			return
		}
		for _, row := range trainingData {
			r.LinearRegression.Train(regression.DataPoint(row[len(row)-1], row[:len(row)-1]))
		}
		r.LinearRegression.Run()
		log.WithFields(log.Fields{
			"TrainInterval": r.CronInterval,
			"DataFile":      r.CSVfile,
		}).Println("Router successfully retrained")
	} else {
		log.WithFields(log.Fields{
			"TrainInterval": r.CronInterval,
			"DataFile":      r.CSVfile,
			"error":         err,
		}).Println("Unable to find past data")
	}
}

// Get the past data from csv file.
func readCSVfile(path string) ([][]float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(bufio.NewReader(f))
	dataPoint := make([][]float64, 0)
	for {
		record, err := r.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}
		var row = make([]float64, len(record))
		for index := range record {
			row[index], _ = strconv.ParseFloat(record[index], 64)
		}
		dataPoint = append(dataPoint, row)
	}
	if len(dataPoint) > 0 {
		return dataPoint, nil
	}
	return nil, errors.New("The length of past data is 0")
}

// Function to get the appropriate source agent
func (r *Router) FindSource(tr *TransferRequest) ([]SourceStats, error) {
	// Find the union of files and files stored per agent
	unionSet, filteredAgent := GetUnionCatalog(tr)
	if len(filteredAgent) <= 0 {
		return nil, errors.New("Couldn't find appropriate agent")
	}
	// Get the prediction values
	for _, agent := range filteredAgent {
		url := fmt.Sprintf("%s/status", agent.SrcUrl)
		resp := utils.FetchResponse(url, []byte{})
		var status AgentStatus
		if resp.Error != nil {
			agent.prediction = MIN_FLOAT
			continue
		}
		err := json.Unmarshal(resp.Data, &status)
		if err != nil {
			agent.prediction = MIN_FLOAT
			continue
		}
		// Predict output through LinearRegression
		result, err := r.LinearRegression.Predict([]float64{status.CpuUsage, status.MemUsage})
		if err != nil {
			agent.prediction = MIN_FLOAT
			continue
		} else {
			agent.prediction = result
		}
	}
	sort.Slice(filteredAgent, func(i, j int) bool {
		return filteredAgent[i].prediction < filteredAgent[j].prediction
	})
	for i := len(filteredAgent) - 1; i >= 0 && unionSet.Size() > 0; i-- {
		commonFiles := set.Intersection(unionSet, filteredAgent[i].catalogSet)
		requests := make([]TransferRequest, 0)
		for _, file := range commonFiles.List() {
			requests = append(requests, TransferRequest{File: file.(string), SrcUrl: tr.SrcUrl, SrcAlias: tr.SrcAlias, DstUrl: tr.DstUrl, DstAlias: tr.DstAlias})
		}
		filteredAgent[i].Requests = requests
		unionSet.Remove(commonFiles)
	}
	return filteredAgent, nil
}

// Function to get the union of files
func GetUnionCatalog(tRequest *TransferRequest) (*set.SetNonTS, []SourceStats) {
	unionSet := set.NewNonTS()
	filteredAgent := make([]SourceStats, 0)
	for srcAlias, srcUrl := range *AgentRouter.Agents {
		records, err := GetRemoteFiles(*tRequest, srcUrl)
		if err != nil || len(records) <= 0 {
			continue
		}
		agentSet := set.NewNonTS()
		for _, catalog := range records {
			agentSet.Add(catalog.Lfn)
		}
		unionSet = set.NewNonTS(set.Union(unionSet, agentSet))
		agentStat := SourceStats{SrcUrl: srcUrl, SrcAlias: srcAlias, catalogSet: agentSet}
		filteredAgent = append(filteredAgent, agentStat)
	}
	return unionSet, filteredAgent
}
