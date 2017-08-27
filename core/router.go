package core

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"
	"github.com/sjwhitworth/golearn/base"
	learn "github.com/sjwhitworth/golearn/linear_models"
	"github.com/vkuznet/transfer2go/utils"
)

type Router struct {
	CronInterval     string                  // Helps to set hourly based cron job
	LinearRegression *learn.LinearRegression // machine learning model
	CSVfile          string                  // historical data file
	Agents           *map[string]string      // list of connected agents
}

// AgentRouter helps to call router's methods
var AgentRouter Router

// newRouter returns new instance of Router type
func NewRouter(interval string, agent *map[string]string) *cron.Cron {
	log.Println(agent)
	timeConfig := "@every " + interval // It works on this format - http://golang.org/pkg/time/#ParseDuration
	lr := learn.NewLinearRegression()
	c := cron.New()
	c.AddFunc(timeConfig, train)
	AgentRouter = Router{CronInterval: interval, LinearRegression: lr, Agents: agent}
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

	err := convertToCSV(dataPoints)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Println("Error while training router")
		return
	}

	trainingData, err := base.ParseCSVToInstances(AgentRouter.CSVfile, false)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Println("Error while training router")
		return
	}

	err = AgentRouter.LinearRegression.Fit(trainingData)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Println("Error while training router")
		return
	}
	log.WithFields(log.Fields{
		"CronInterval": AgentRouter.CronInterval,
		"Data":         AgentRouter.CSVfile,
	}).Println("Router successfully retrained")
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
	if _, err := os.Stat(AgentRouter.CSVfile); !os.IsNotExist(err) {
		trainingData, err := base.ParseCSVToInstances(AgentRouter.CSVfile, false)
		if err != nil {
			log.WithFields(log.Fields{
				"Error": err,
			}).Println("Error while training router")
			return
		}
		err = AgentRouter.LinearRegression.Fit(trainingData)
		if err != nil {
			log.WithFields(log.Fields{
				"Error": err,
			}).Println("Error while training router")
			return
		}
		log.WithFields(log.Fields{
			"TrainInterval": AgentRouter.CronInterval,
			"DataFile":      AgentRouter.CSVfile,
		}).Println("Router successfully retrained")
	} else {
		log.WithFields(log.Fields{
			"TrainInterval": AgentRouter.CronInterval,
			"DataFile":      AgentRouter.CSVfile,
			"error":         err,
		}).Println("Unable to find past data")
	}
}

// Function to get the appropriate source agent
func (r *Router) FindSource(tRequest *TransferRequest) (string, string, error) {
	return "test.com", "test", nil
}
