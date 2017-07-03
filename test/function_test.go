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

// Struct is to make the test cases
type tests struct {
	description        string
	url                string
	expectedStatusCode int
	expectedBody       string
	result             bool
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
	req := core.TransferRequest{SrcUrl: "http://localhost:8989", SrcAlias: "Test", File: "file.root", DstUrl: "http://localhost:8000", DstAlias: "Test2", Priority: 1}
	furl := req.SrcUrl + "/request"
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
		expectedBody:       url,
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

// Test /files endpoint. Returns list of files in the agent
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
