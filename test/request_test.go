package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vkuznet/transfer2go/core"
	"github.com/vkuznet/transfer2go/utils"
)

var url = "http://localhost:8989"

type tests struct {
	description        string
	url                string
	expectedStatusCode int
	expectedBody       string
	result             bool
}

// Function names are according to api endpoints

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

func TestAgents(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Test the list of connected agents",
		url:                url + "/agents",
		expectedStatusCode: 200,
		expectedBody:       "http://localhost:8989",
	}

	var data map[string]interface{}

	resp, err := http.Get(test.url)
	actual, err := ioutil.ReadAll(resp.Body)

	defer resp.Body.Close()
	json.Unmarshal([]byte(actual), &data)
	assert.NoError(err)

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	assert.Equal(test.expectedBody, data["Test"], test.description)

}

func TestWriteTFC(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Check TFC upload functionality",
		url:                url + "/tfc",
		expectedStatusCode: 200,
		expectedBody:       "http://localhost:8989",
	}

	f, err := os.Create("data/testdata.txt")
	d2 := []byte{115, 111, 109, 101, 10}
	_, err = f.Write(d2)
	assert.NoError(err)
	f.Close()

	fname := "data/records.json"
	c, err := ioutil.ReadFile(fname)
	assert.NoError(err)

	var records []core.CatalogEntry
	err = json.Unmarshal([]byte(c), &records)
	assert.NoError(err)

	d, err := json.Marshal(records)
	assert.NoError(err)

	des := url + "/tfc"
	resp := utils.FetchResponse(des, d)

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)

	err = os.Remove("data/testdata.txt")
	assert.NoError(err)

}

func TestFiles(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Get the list of files",
		url:                url + "/files",
		expectedStatusCode: 200,
		expectedBody:       "[\"/store/file.root\"]",
	}

	resp, err := http.Get(test.url)
	actual, err := ioutil.ReadAll(resp.Body)
	assert.NoError(err)
	defer resp.Body.Close()

	assert.Equal(test.expectedStatusCode, resp.StatusCode, test.description)
	assert.Equal(test.expectedBody, string(actual), test.description)

}

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

func TestLfnLookup(t *testing.T) {
	assert := assert.New(t)

	test := tests{
		description:        "Get TFC records",
		url:                url + "/tfc?dataset=/a/b/c",
		expectedStatusCode: 200,
		expectedBody:       "/store/file.root",
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
