package main

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

var url = "http://localhost:8989"

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

// Test /agent end point
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
	assert.Equal(test.expectedBody, data["Test2"], test.description)

}

// Test /tfc endpoint
func TestWriteTFC(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Check TFC upload functionality",
		url:                url + "/tfc",
		expectedStatusCode: 200,
		expectedBody:       "http://localhost:8989",
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

	des := url + "/tfc"
	resp := utils.FetchResponse(des, d)

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)

}

// Test /files endpoint
func TestFiles(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Get the list of files",
		url:                url + "/files",
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

// Test /tfc get endpoint
func TestReadTFC(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Get TFC records",
		url:                url + "/tfc",
		expectedStatusCode: 200,
		expectedBody:       "test/data/testdata.txt",
	}

	var data []map[string]interface{}

	resp, err := http.Get(test.url)
	actual, err := ioutil.ReadAll(resp.Body)

	defer resp.Body.Close()
	json.Unmarshal([]byte(actual), &data)
	assert.NoError(err)

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	assert.Equal(test.expectedBody, data[0]["pfn"], test.description)

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

// Transfer file from test to test2 using cp protocol
func TestTransferRequest(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Transfer file using cp protocol",
		url:                url + "/request",
		expectedStatusCode: 200,
		expectedBody:       "",
	}

	var requests []core.TransferRequest
	req := core.TransferRequest{SrcUrl: "http://localhost:8989", SrcAlias: "Test", File: "file.root", DstUrl: "http://localhost:8000", DstAlias: "Test2"}
	furl := req.SrcUrl + "/request"
	requests = append(requests, req)
	d, err := json.Marshal(requests)
	assert.NoError(err)

	resp := utils.FetchResponse(furl, d)
	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	time.Sleep(time.Second * 2)

	err = deleteFile("data/testdata.txt")
	assert.NoError(err)
	err = deleteFile("file.root")
	assert.NoError(err)

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
