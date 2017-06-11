package test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
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
func TestTransferRequest(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Transfer Request endpoint",
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
}

func TestList(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Get all the list of requests",
		url:                url + "/list?type=pending",
		expectedStatusCode: 200,
		expectedBody:       url,
	}

	var data []map[string]map[string]interface{}
	resp, err := http.Get(test.url)
	assert.NoError(err)
	actual, err := ioutil.ReadAll(resp.Body)
	assert.NoError(err)
	defer resp.Body.Close()
	json.Unmarshal([]byte(actual), &data)
	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	assert.Equal(test.expectedBody, data[0]["Value"]["srcUrl"], test.description)
}
