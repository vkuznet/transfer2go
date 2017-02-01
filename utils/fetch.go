package utils

// transfer2go/utils - Go utilities for transfer2go
//
// Copyright (c) 2017 - Valentin Kuznetsov <vkuznet@gmail.com>

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/user"
	"regexp"
	"runtime"
	"time"

	"github.com/vkuznet/x509proxy"
)

// VERBOSE variable control verbosity level of client's utilities
var VERBOSE int

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
	if VERBOSE > 0 {
		fmt.Println("uproxy", uproxy)
		fmt.Println("uckey", uckey)
		fmt.Println("ucert", ucert)
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

// create global HTTP client and re-use it through the code
var client = HttpClient()

// ResponseType structure is what we expect to get for our URL call.
// It contains a request URL, the data chunk and possible error from remote
type ResponseType struct {
	Url        string // response url
	Data       []byte // response data, i.e. what we got with Body of the response
	Error      error  // http error, a non-2xx return code is not an error
	Status     string // http status string
	StatusCode int    // http status code
}

func (r *ResponseType) String() string {
	return fmt.Sprintf("<Response: url=%s, data=%s, error=%v>", r.Url, string(r.Data), r.Error)
}

// UrlRequest structure holds details about url request's attributes
type UrlRequest struct {
	rurl string
	args string
	out  chan<- ResponseType
	ts   int64
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
			fmt.Println("Unable to make GET request", e)
		}
		req.Header.Add("Accept", "*/*")
	}
	if VERBOSE > 1 {
		dump1, err1 := httputil.DumpRequestOut(req, true)
		fmt.Println("### HTTP request", req, string(dump1), err1)
	}
	resp, err := client.Do(req)
	response.Status = resp.Status
	response.StatusCode = resp.StatusCode
	if VERBOSE > 0 {
		if len(args) > 0 {
			fmt.Println("TRANSFER2GO POST", rurl, string(args), err, time.Now().Sub(startTime))
		} else {
			fmt.Println("TRANSFER2GO GET", rurl, string(args), err, time.Now().Sub(startTime))
		}
	}
	if VERBOSE > 1 {
		dump2, err2 := httputil.DumpResponse(resp, true)
		fmt.Println("### HTTP response", string(dump2), err2)
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
		fmt.Println("TRANSFER2GO WARNING, fail to fetch data", rurl, "error", resp.Error)
		for i := 1; i <= urlRetry; i++ {
			sleep := time.Duration(i) * time.Second
			time.Sleep(sleep)
			r = FetchResponse(rurl, args)
			if r.Error == nil {
				break
			}
			fmt.Println("TRANSFER2GO WARNING", rurl, "retry", i, "error", r.Error)
		}
		resp = r
	}
	if resp.Error != nil {
		fmt.Println("TRANSFER2GO ERROR, fail to fetch data", rurl, "retries", urlRetry, "error", resp.Error)
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
		fmt.Println("ERROR invalid URL:", rurl)
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

// Hash implements hash function for data, it returns a hash and number of bytes
func Hash(data []byte) (string, int64) {
	hasher := sha256.New()
	b, e := hasher.Write(data)
	if e != nil {
		log.Println("ERROR, Unable to write chunk of data via hasher.Write", e)
	}
	return hex.EncodeToString(hasher.Sum(nil)), int64(b)
}

// Stack helper function to return Stack
func Stack() string {
	trace := make([]byte, 2048)
	count := runtime.Stack(trace, false)
	return fmt.Sprintf("\nStack of %d bytes: %s\n", count, trace)
}

// ErrPropagate error helper function which can be used in defer ErrPropagate()
func ErrPropagate(api string) {
	if err := recover(); err != nil {
		log.Println("DAS ERROR", api, "error", err, Stack())
		panic(fmt.Sprintf("%s:%s", api, err))
	}
}

// ErrPropagate2Channel error helper function which can be used in goroutines as
// ch := make(chan interface{})
// go func() {
//    defer ErrPropagate2Channel(api, ch)
//    someFunction()
// }()
func ErrPropagate2Channel(api string, ch chan interface{}) {
	if err := recover(); err != nil {
		log.Println("DAS ERROR", api, "error", err, Stack())
		ch <- fmt.Sprintf("%s:%s", api, err)
	}
}

// GoDeferFunc helper function to run any given function in defered go routine
func GoDeferFunc(api string, f func()) {
	ch := make(chan interface{})
	go func() {
		defer ErrPropagate2Channel(api, ch)
		f()
		ch <- "ok" // send to channel that we can read it later in case of success of f()
	}()
	err := <-ch
	if err != nil && err != "ok" {
		panic(err)
	}
}

// FindInList helper function to find item in a list
func FindInList(a string, arr []string) bool {
	for _, e := range arr {
		if e == a {
			return true
		}
	}
	return false
}

// InList helper function to check item in a list
func InList(a string, list []string) bool {
	check := 0
	for _, b := range list {
		if b == a {
			check += 1
		}
	}
	if check != 0 {
		return true
	}
	return false
}

// MapKeys helper function to return keys from a map
func MapKeys(rec map[string]interface{}) []string {
	keys := make([]string, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	return keys
}

// List2Set helper function to convert input list into set
func List2Set(arr []string) []string {
	var out []string
	for _, key := range arr {
		if !InList(key, out) {
			out = append(out, key)
		}
	}
	return out
}

// Provides a list of host IPs
func HostIP() []string {
	var out []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println("ERROR unable to resolve net.InterfaceAddrs", err)
	}
	for _, addr := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				out = append(out, ipnet.IP.String())
			}
			if ipnet.IP.To16() != nil {
				out = append(out, ipnet.IP.String())
			}
		}
	}
	return List2Set(out)
}
