package client

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/vkuznet/x509proxy"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"os"
	"os/user"
	"regexp"
	"strings"
	"time"
)

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
	Url   string
	Data  []byte
	Error error
}

// UrlRequest structure holds details about url request's attributes
type UrlRequest struct {
	rurl string
	args string
	out  chan<- ResponseType
	ts   int64
}

// FetchResponse fetches data for provided URL, args is a json dump of arguments
func FetchResponse(rurl, args string) ResponseType {
	startTime := time.Now()
	var response ResponseType
	response.Url = rurl
	if validateUrl(rurl) == false {
		response.Error = errors.New("Invalid URL")
		return response
	}
	var req *http.Request
	if len(args) > 0 {
		jsonStr := []byte(args)
		req, _ = http.NewRequest("POST", rurl, bytes.NewBuffer(jsonStr))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, _ = http.NewRequest("GET", rurl, nil)
		req.Header.Add("Accept-Encoding", "identity")
		if strings.Contains(rurl, "sitedb") || strings.Contains(rurl, "reqmgr") {
			req.Header.Add("Accept", "application/json")
		}
	}
	if VERBOSE > 1 {
		dump1, err1 := httputil.DumpRequestOut(req, true)
		fmt.Println("### HTTP request", req, string(dump1), err1)
	}
	resp, err := client.Do(req)
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
	if VERBOSE > 0 {
		if args == "" {
			fmt.Println("TRANSFER2GO GET", rurl, time.Now().Sub(startTime))
		} else {
			fmt.Println("TRANSFER2GO POST", rurl, args, time.Now().Sub(startTime))
		}
	}
	return response
}

// Fetch data for provided URL and redirect results to given channel
func Fetch(rurl string, args string, ch chan<- ResponseType) {
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
