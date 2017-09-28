package test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/vkuznet/transfer2go/core"
)

func caller(agent, src, dst string) {
	fmt.Println("caller", agent, src, dst)
}

// TestCentralCatalog test core.TestCentralCatalog functionality
func TestCentralCatalog(t *testing.T) {
	path := "/tmp/transfer2go/central"
	cc := core.CentralCatalog{Path: path}
	ts := time.Now().Unix()
	records := []string{fmt.Sprintf("bla-1-%d", ts), fmt.Sprintf("bla-2-%d", ts)}
	err := cc.Put("table", records)
	if err != nil {
		t.Error(err)
	}
	data, err := cc.Get("table")
	var docs []string
	var row string
	for _, v := range data {
		if string(v) == "\n" {
			docs = append(docs, row)
			row = ""
		} else {
			row += string(v)
		}
	}
	fmt.Println("Records")
	for _, v := range records {
		fmt.Println(v)
	}
	fmt.Println("Docs")
	for _, v := range docs {
		fmt.Println(v)
	}
	if len(records) != len(docs) {
		t.Error("Data do not match")
	}
	err = os.RemoveAll(path)
	if err != nil {
		t.Error(err)
	}
}
