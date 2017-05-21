package utils

// transfer2go/utils - Go utilities for transfer2go
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"os"
	"os/user"
	"regexp"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vkuznet/x509proxy"
)

// ResponseType structure is what we expect to get for our URL call.
// It contains a request URL, the data chunk and possible error from remote
type ResponseType struct {
	Url        string // response url
	Data       []byte // response data, i.e. what we got with Body of the response
	Error      error  // http error, a non-2xx return code is not an error
	Status     string // http status string
	StatusCode int    // http status code
}

// UrlRequest structure holds details about url request's attributes
type UrlRequest struct {
	rurl string
	args string
	out  chan<- ResponseType
	ts   int64
}

// VERBOSE variable control verbosity level of client's utilities
var VERBOSE int

// create global HTTP client and re-use it through the code
var client = HttpClient()

// UserDN function parses user Distinguished Name (DN) from client's HTTP request
func UserDN(r *http.Request) string {
	var names []interface{}
	for _, cert := range r.TLS.PeerCertificates {
		for _, name := range cert.Subject.Names {
			switch v := name.Value.(type) {
			case string:
				names = append(names, v)
			}
		}
	}
	parts := names[:7]
	return fmt.Sprintf("/DC=%s/DC=%s/OU=%s/OU=%s/CN=%s/CN=%s/CN=%s", parts...)
}

// client X509 certificates
func tlsCerts() ([]tls.Certificate, error) {
	uproxy := os.Getenv("X509_USER_PROXY")
	uckey := os.Getenv("X509_USER_KEY")
	ucert := os.Getenv("X509_USER_CERT")

	// check if /tmp/x509up_u$UID exists, if so setup X509_USER_PROXY env
	u, err := user.Current()
	if err == nil {
		fname := fmt.Sprintf("/tmp/x509up_u%s", u.Uid)
		if _, err := os.Stat(fname); err == nil {
			uproxy = fname
		}
	}
	if VERBOSE > 1 {
		log.WithFields(log.Fields{
			"uproxy": uproxy,
		}).Println("")
		log.WithFields(log.Fields{
			"uckey": uckey,
		}).Println("")
		log.WithFields(log.Fields{
			"ucert": ucert,
		}).Println("")
	}

	if uproxy == "" && uckey == "" { // user doesn't have neither proxy or user certs
		return nil, nil
	}
	if uproxy != "" {
		// use local implementation of LoadX409KeyPair instead of tls one
		x509cert, err := x509proxy.LoadX509Proxy(uproxy)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proxy X509 proxy set by X509_USER_PROXY: %v", err)
		}
		return []tls.Certificate{x509cert}, nil
	}
	x509cert, err := tls.LoadX509KeyPair(ucert, uckey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse user X509 certificate: %v", err)
	}
	return []tls.Certificate{x509cert}, nil
}

// HttpClient is HTTP client for urlfetch server
func HttpClient() (client *http.Client) {
	// get X509 certs
	certs, err := tlsCerts()
	if err != nil {
		panic(err.Error())
	}
	if len(certs) == 0 {
		client = &http.Client{}
		return
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{Certificates: certs,
			InsecureSkipVerify: true},
	}
	client = &http.Client{Transport: tr}
	return
}

func (r *ResponseType) String() string {
	return fmt.Sprintf("<Response: url=%s data=%s error=%v>", r.Url, string(r.Data), r.Error)
}

// FetchResponse fetches data for provided URL, args is a json dump of arguments
func FetchResponse(rurl string, args []byte) ResponseType {
	startTime := time.Now()
	var response ResponseType
	response.Url = rurl
	if validateUrl(rurl) == false {
		response.Error = errors.New("Invalid URL")
		return response
	}
	var req *http.Request
	var e error
	if len(args) > 0 {
		req, e = http.NewRequest("POST", rurl, bytes.NewBuffer(args))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, e = http.NewRequest("GET", rurl, nil)
		if e != nil {
			log.WithFields(log.Fields{
				"Error": e,
			}).Error("Unable to make GET request")
		}
		req.Header.Add("Accept", "*/*")
	}
	if VERBOSE > 1 {
		dump1, err1 := httputil.DumpRequestOut(req, true)
		log.WithFields(log.Fields{
			"Request": req,
			"Dump":    string(dump1),
			"Error":   err1,
		}).Println("HTTP request")
	}
	resp, err := client.Do(req)
	if err != nil {
		log.WithFields(log.Fields{
			"Error": err,
		}).Error("HTTP", err)
		response.Error = err
		return response
	}
	response.Status = resp.Status
	response.StatusCode = resp.StatusCode
	if VERBOSE > 0 {
		if len(args) > 0 {
			log.WithFields(log.Fields{
				"URL":   rurl,
				"Args":  string(args),
				"Error": err,
				"Time":  time.Now().Sub(startTime),
			}).Println("HTTP POST")
		} else {
			log.WithFields(log.Fields{
				"URL":   rurl,
				"Args":  string(args),
				"Error": err,
				"Time":  time.Now().Sub(startTime),
			}).Println("HTTP GET")
		}
	}
	if VERBOSE > 1 {
		dump2, err2 := httputil.DumpResponse(resp, true)
		log.WithFields(log.Fields{
			"Dump":  string(dump2),
			"Error": err2,
		}).Println("HTTP response")
	}
	if err != nil {
		response.Error = err
		return response
	}
	defer resp.Body.Close()
	response.Data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		response.Error = err
	}
	return response
}

// Fetch data for provided URL and redirect results to given channel
func Fetch(rurl string, args []byte, ch chan<- ResponseType) {
	urlRetry := 3
	var resp, r ResponseType
	resp = FetchResponse(rurl, args)
	if resp.Error != nil {
		log.WithFields(log.Fields{
			"URL":   rurl,
			"Error": resp.Error,
		}).Warn("Fail to fetch data")
		for i := 1; i <= urlRetry; i++ {
			sleep := time.Duration(i) * time.Second
			time.Sleep(sleep)
			r = FetchResponse(rurl, args)
			if r.Error == nil {
				break
			}
			log.WithFields(log.Fields{
				"URL":   rurl,
				"retry": i,
				"Error": r.Error,
			}).Warn("TRANSFER2GO")
		}
		resp = r
	}
	if resp.Error != nil {
		log.WithFields(log.Fields{
			"URL":   rurl,
			"Retry": urlRetry,
			"Error": resp.Error,
		}).Error("TRANSFER2GO, fail to fetch data")
	}
	ch <- resp
}

// Helper function which validates given URL
func validateUrl(rurl string) bool {
	if len(rurl) > 0 {
		pat := "(https|http)://[-A-Za-z0-9_+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]"
		matched, err := regexp.MatchString(pat, rurl)
		if err == nil {
			if matched == true {
				return true
			}
		}
		log.WithFields(log.Fields{
			"URL": rurl,
		}).Error("Invalid URL:", rurl)
	}
	return false
}

// Response represents final response in a form of JSON structure
// we use custorm representation
func Response(rurl string, data []byte) []byte {
	b := []byte(`{"url":`)
	u := []byte(rurl)
	c := []byte(",")
	d := []byte(`"data":`)
	e := []byte(`}`)
	a := [][]byte{b, u, c, d, data, e}
	s := []byte(" ")
	r := bytes.Join(a, s)
	return r

}
