package server

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"
	learn "github.com/sjwhitworth/golearn/linear_models"
	"github.com/vkuznet/transfer2go/core"
	"github.com/vkuznet/transfer2go/utils"
)

type Router struct {
	CronInterval     string                  // Helps to set hourly based cron job
	LinearRegression *learn.LinearRegression // machine learning model
	CronJob          *cron.Cron              // Cron job instance
}

// newRouter returns new instance of Router type
func newRouter(interval string) *Router {
	timeConfig := "@every " + interval // It works on this format - http://golang.org/pkg/time/#ParseDuration
	lr := learn.NewLinearRegression()
	c := cron.New()
	c.AddFunc(timeConfig, train)
	return &Router{CronInterval: interval, LinearRegression: lr, CronJob: c}
}

// Function to train the agent
func train() {
	var dataPoints []core.TransferData
	for _, source := range _agents {
		data, err := getHistory(source)
		if err == nil {
			dataPoints = append(dataPoints, data...)
		}
	}
	err := writeToCSV(dataPoints)
	if err != nil {
		// TODO: retry upto maxTry.
	}
	log.Println("Cron fired")
}

// Convert struct to csv format
func writeToCSV(dataPoints []core.TransferData) error {
	file, err := os.Create(_config.Cfile)
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
func getHistory(source string) ([]core.TransferData, error) {
	url := fmt.Sprintf("%s/history?duration=%s", source, AgentRouter.CronInterval)
	resp := utils.FetchResponse(url, []byte{})
	if resp.Error != nil {
		return nil, resp.Error
	}
	var transferRecords []core.TransferData
	err := json.Unmarshal(resp.Data, &transferRecords)
	if err != nil {
		return nil, err
	}
	return transferRecords, nil
}

// TODO: Function to iterate through all the connected agents

// TODO: Function to get the historical data

// TODO: Function to train the model

// TODO: Function to do the cron job
