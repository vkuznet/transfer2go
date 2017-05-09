package main

import (
	"net/http"
	"testing"
  "io/ioutil"

	"github.com/stretchr/testify/assert"
)

var url = "http://localhost:8989"

func TestGetReposHandler(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		description        string
		url                string
		expectedStatusCode int
		expectedBody       string
	}{
		{
			description:        "missing argument user",
			url:                url + "/status",
			expectedStatusCode: 200,
			expectedBody: "{\"url\":\"http://localhost:8989\",\"name\":\"Test\",\"ts\":1494326276,\"catalog\":\"sqlite3\",\"protocol\":\"cp\",\"backend\":\"/tmp/vk\",\"tool\":\"/bin/cp\",\"toolopts\":\"\",\"agents\":{\"Test\":\"http://localhost:8989\"},\"addrs\":[\"fe80::1\",\"fe80::c2ce:cdff:feea:5421\",\"192.168.1.40\",\"fe80::3425:6dff:fe98:2642\"],\"metrics\":{\"bytes\":0,\"failed\":0,\"in\":0,\"total\":0,\"totalBytes\":0}}",
    },
	}

	for _, tc := range tests {

		resp, err := http.Get(tc.url)
    actual, err := ioutil.ReadAll(resp.Body)

		assert.NoError(err)

		assert.Equal(tc.expectedStatusCode, resp.StatusCode, tc.description)
		assert.Equal(tc.expectedBody, string(actual), tc.description)
	}
}
