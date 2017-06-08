package test

import (
	"encoding/json"
	"testing"

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
}
