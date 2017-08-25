package server

import (
	"encoding/json"
	"fmt"

	"github.com/robfig/cron"
	"github.com/vkuznet/transfer2go/core"
	"github.com/vkuznet/transfer2go/utils"
)

type Router struct {
	CronInterval string `json:"interval"` // Helps to set hourly based cron job
}

// AgentRouter helps to call router's methods
var AgentRouter Router

// newRouter returns new instance of Router type
func newRouter(interval string) *cron.Cron {
	timeConfig := "@every " + interval // It works on this format - http://golang.org/pkg/time/#ParseDuration
	AgentRouter = Router{CronInterval: interval}
	c := cron.New()
	c.AddFunc(timeConfig, train)
	c.Start()
	return c
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
