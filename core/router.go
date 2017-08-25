package core

import (
	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"
)

type Router struct {
	CronInterval string `json:"interval"` // Helps to set hourly based cron job
}

// AgentRouter helps to call router's methods
var AgentRouter Router

// NewRouter returns new instance of Router type
func NewRouter(interval string) *cron.Cron {
	AgentRouter = Router{CronInterval: interval}
	c := cron.New()
	c.AddFunc(interval, AgentRouter.Train)
	c.Start()
	return c
}

// TODO: Function to train the agent
func (r *Router) Train() {
	log.Println("Training")
}

// TODO: Function to iterate through all the connected agents

// TODO: Function to get the historical data

// TODO: Function to train the model

// TODO: Function to do the cron job
