package test

import (
	"fmt"
	"testing"

	//     "github.com/stretchr/testify/assert"

	"github.com/vkuznet/transfer2go/core"
)

func caller(agent, src, dst string) {
	fmt.Println("caller", agent, src, dst)
}

// TestAuthDecorator test core.AuthDecorator function
func TestAuthzDecorator(t *testing.T) {
	//     assert := assert.New(t)
	agent := "Agent"
	src := "Source"
	dst := "Destination"
	core.AuthzDecorator(caller, "cms")(agent, src, dst)
	//     assert.Equal(1, 1, "values should be equal")
}
