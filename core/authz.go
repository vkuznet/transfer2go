package core

// transfer2go auth data transfer module
// Author - Valentin Kuznetsov <vkuznet@gmail.com>

import logs "github.com/sirupsen/logrus"

// CallerFunc type func(string, string, string)
type CallerFunc func(agent, src, dst string)

// AuthzDecorator provides skeleton for performing authorization check with given function
func AuthzDecorator(fn CallerFunc, policy string) CallerFunc {
	return func(agent, src, dst string) {
		// implement logic of authorization
		logs.WithFields(logs.Fields{
			"Function":    fn,
			"Agent":       agent,
			"Source":      src,
			"Destination": dst,
			"Policy":      policy,
		}).Info("Calling AuthzDecorator")
		fn(agent, src, dst)
	}
}
