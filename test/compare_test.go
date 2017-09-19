package test

import (
	"strconv"
	"testing"
	"time"

	"github.com/vkuznet/transfer2go/core"
)

// Compare 1k/10k files of source with the same 1k files of destination
func TestCompare(t *testing.T) {
	input := [2]int{1000, 10000}
	for _, val := range input {
		var sourceCatalog []core.CatalogEntry
		destinationCatalog := make([]core.CatalogEntry, val)
		dataset := "/a/b/c"
		block := "/a/b/c#123"
		for i := 0; i < val; i++ {
			rec := core.CatalogEntry{Dataset: dataset, Block: block, Lfn: block + "-" + dataset + "file" + strconv.Itoa(i) + ".root"}
			sourceCatalog = append(sourceCatalog, rec)
		}
		copy(destinationCatalog[:], sourceCatalog)
		start := time.Now()

		records := core.CompareRecords(sourceCatalog, destinationCatalog)
		elapsed := time.Since(start)
		t.Logf("For %d it took %s", val, elapsed)
		if records != nil {
			t.Errorf("Incorrect Match for 1k files: %d", len(records))
		}
	}
}

// Compare 1k files of source with the distinct 10k files of destination
func TestCompareTenThousandUncommon(t *testing.T) {
	var sourceCatalog []core.CatalogEntry
	var destinationCatalog []core.CatalogEntry
	dataset := "/a/b/c"
	block := "/a/b/c#123"
	for i := 0; i < 10000; i++ {
		rec := core.CatalogEntry{Dataset: dataset, Block: block, Lfn: block + "-" + dataset + "file" + strconv.Itoa(i) + ".root"}
		sourceCatalog = append(sourceCatalog, rec)
	}
	start := time.Now()
	records := core.CompareRecords(sourceCatalog, destinationCatalog)
	elapsed := time.Since(start)
	t.Log("For 10k uncommon it took", elapsed)
	if records != nil {
		t.Log("Need to transfer total files:", len(records))
	}
}

// Test Request function
func TestGetDestFiles(t *testing.T) {
	start := time.Now()
	rec, err := core.GetDestFiles(core.TransferRequest{SrcUrl: "http://localhost:8000", DstUrl: "http://localhost:9000", Dataset: "/a/b/c"})
	elapsed := time.Since(start)
	t.Log("Time took to get destination file", elapsed)
	if err != nil {
		t.Log("Error in TestGetDestFiles:", err)
	}
	t.Log("Files found on destination:", len(rec))
}
