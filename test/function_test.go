package test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vkuznet/transfer2go/core"
	"github.com/vkuznet/transfer2go/utils"
)

// Start single server instace and then run this tests.
// Command go test function_test.go

var url = "http://localhost:8989"
var sourceURL = "http://localhost:8000"
var destinationURL = "http://localhost:9000"

// Struct is to make the test cases
type tests struct {
	description        string
	url                string
	expectedStatusCode int
	expectedBody       string
	result             bool
}

// Check status of agent
func TestStatus(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Check status of server",
		url:                url + "/status",
		expectedStatusCode: 200,
		expectedBody:       "Test",
	}

	var data map[string]interface{}

	resp, err := http.Get(test.url)
	actual, err := ioutil.ReadAll(resp.Body)

	defer resp.Body.Close()
	json.Unmarshal([]byte(actual), &data)
	assert.NoError(err)

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	assert.Equal(test.expectedBody, data["name"], test.description)

}

// Test /agent end point. Returns list of registered agents
func TestAgents(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Test the list of connected agents",
		url:                url + "/agents",
		expectedStatusCode: 200,
		expectedBody:       "http://localhost:8000",
	}

	var data map[string]interface{}

	resp, err := http.Get(test.url)
	actual, err := ioutil.ReadAll(resp.Body)

	defer resp.Body.Close()
	json.Unmarshal([]byte(actual), &data)
	assert.NoError(err)

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	assert.Equal(test.expectedBody, data["source"], test.description)

}

// This function helps to register fake requests
func TestRegister(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Transfer Request endpoint",
		url:                url + "/request",
		expectedStatusCode: 200,
		expectedBody:       "",
	}

	var requests []core.TransferRequest
	req := core.TransferRequest{SrcUrl: "http://localhost:8000", SrcAlias: "Test", File: "file.root", DstUrl: "http://localhost:9000", DstAlias: "Test2", Priority: 1}
	furl := url + "/request"
	requests = append(requests, req)
	d, err := json.Marshal(requests)
	assert.NoError(err)

	resp := utils.FetchResponse(furl, d)
	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	time.Sleep(time.Second * 2)
}

// Get the list of pending requests
func TestList(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Get all the pending list of requests",
		url:                url + "/list?type=pending",
		expectedStatusCode: 200,
		expectedBody:       sourceURL,
	}

	var data []map[string]interface{}
	resp, err := http.Get(test.url)
	assert.NoError(err)

	actual, err := ioutil.ReadAll(resp.Body)
	assert.NoError(err)

	defer resp.Body.Close()
	json.Unmarshal([]byte(actual), &data)
	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	assert.Equal(test.expectedBody, data[0]["srcUrl"], test.description)
}

// Upload file
func TestWriteTFC(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Check file upload functionality",
		url:                url + "/tfc",
		expectedStatusCode: 200,
		expectedBody:       "http://localhost:8000",
	}

	err := createFile("data/testdata.txt")
	assert.NoError(err)

	fname := "data/records.json"
	c, err := ioutil.ReadFile(fname)
	assert.NoError(err)
	time.Sleep(time.Second * 2)

	var records []core.CatalogEntry
	err = json.Unmarshal([]byte(c), &records)
	assert.NoError(err)

	d, err := json.Marshal(records)
	assert.NoError(err)

	des := sourceURL + "/tfc"
	resp := utils.FetchResponse(des, d)

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
}

// Test /files endpoint. Returns list of files in the agent
func TestFiles(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Get the list of files",
		url:                sourceURL + "/files",
		expectedStatusCode: 200,
		expectedBody:       "[\"file.root\"]",
	}

	resp, err := http.Get(test.url)
	actual, err := ioutil.ReadAll(resp.Body)
	assert.NoError(err)
	defer resp.Body.Close()

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	assert.Equal(test.expectedBody, string(actual), test.description)
}

// Test /action endpoint. Do transfer process.
func TestApproval(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Get the list of files",
		url:                url,
		expectedStatusCode: 200,
		expectedBody:       "",
	}

	// Get pending requests
	var data = []core.TransferRequest{}
	resp, err := http.Get(test.url + "/list?type=pending")
	assert.NoError(err)
	actual, err := ioutil.ReadAll(resp.Body)
	assert.NoError(err)
	defer resp.Body.Close()
	err = json.Unmarshal([]byte(actual), &data)
	assert.NoError(err)
	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)

	// Send pull transfer job to main agent.
	var job []core.Job
	id := data[0].Id
	req := core.TransferRequest{Id: data[0].Id}
	action := core.Job{TransferRequest: req, Action: "pulltransfer"}
	furl := url + "/action"
	job = append(job, action)
	d, err := json.Marshal(job)
	assert.NoError(err)
	output := utils.FetchResponse(furl, d)
	assert.Equal(test.expectedStatusCode, output.StatusCode, test.description)

	time.Sleep(time.Second * 3)

	// Get finished jobs
	data = []core.TransferRequest{}
	resp, err = http.Get(test.url + "/list?type=finished")
	assert.NoError(err)
	actual, err = ioutil.ReadAll(resp.Body)
	assert.NoError(err)
	defer resp.Body.Close()
	err = json.Unmarshal([]byte(actual), &data)
	assert.NoError(err)
	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)

	// Check if our job is finished or not
	assert.Equal(id, data[len(data)-1].Id, test.description)
}

// Test /tfc get endpoint. Return list of TFC records
func TestReadTFC(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Get TFC records",
		url:                destinationURL + "/tfc",
		expectedStatusCode: 200,
		expectedBody:       "test/file.root",
	}

	var data []map[string]interface{}

	resp, err := http.Get(test.url)
	actual, err := ioutil.ReadAll(resp.Body)

	defer resp.Body.Close()
	json.Unmarshal([]byte(actual), &data)
	assert.NoError(err)

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	assert.Equal(test.expectedBody, data[0]["pfn"], test.description)

	err = deleteFile("data/testdata.txt")
	assert.NoError(err)
	err = deleteFile("file.root")
	assert.NoError(err)
}

// Database lookup by dataset
func TestLfnLookup(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Get TFC records",
		url:                url + "/tfc?dataset=/a/b/c",
		expectedStatusCode: 200,
		expectedBody:       "file.root",
	}

	var data []map[string]interface{}

	resp, err := http.Get(test.url)
	actual, err := ioutil.ReadAll(resp.Body)

	defer resp.Body.Close()
	json.Unmarshal([]byte(actual), &data)
	assert.NoError(err)

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	assert.Equal(test.expectedBody, data[0]["lfn"], test.description)
}

// Reset protocol to default(http)
func TestReset(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Reset protocol",
		url:                url + "/reset",
		expectedStatusCode: 200,
		expectedBody:       "",
	}

	resp, err := http.Get(test.url)
	assert.NoError(err)
	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)

}

// Helper function to create test data
func createFile(path string) error {
	f, err := os.Create(path)

	if err != nil {
		return err
	}

	d2 := []byte{115, 111, 109, 101, 10}
	_, err = f.Write(d2)
	f.Close()
	return err
}

func deleteFile(path string) error {
	err := os.Remove(path)
	return err
}
