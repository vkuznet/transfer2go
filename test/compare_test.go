package test

import (
	"strconv"
	"testing"
	"time"

	"github.com/vkuznet/transfer2go/core"
)

// Compare 1k files of source with the same 1k files of destination
func TestCompareThousand(t *testing.T) {
	var sourceCatalog []core.CatalogEntry
	destinationCatalog := make([]core.CatalogEntry, 1000)
	dataset := "/a/b/c"
	block := "/a/b/c#123"
	for i := 0; i < 1000; i++ {
		rec := core.CatalogEntry{Dataset: dataset, Block: block, Lfn: block + "-" + dataset + "file" + strconv.Itoa(i) + ".root"}
		sourceCatalog = append(sourceCatalog, rec)
	}
	copy(destinationCatalog[:], sourceCatalog)
	start := time.Now()
	records := core.CompareRecords(sourceCatalog, destinationCatalog)
	elapsed := time.Since(start)
	t.Log("For 1k it took", elapsed)
	if records != nil {
		t.Errorf("Incorrect Match for 1k files: %d", len(records))
	}
}

// Compare 10k files of source with the same 10k files of destination
func TestCompareTenThousand(t *testing.T) {
	var sourceCatalog []core.CatalogEntry
	destinationCatalog := make([]core.CatalogEntry, 10000)
	dataset := "/a/b/c"
	block := "/a/b/c#123"
	for i := 0; i < 10000; i++ {
		rec := core.CatalogEntry{Dataset: dataset, Block: block, Lfn: block + "-" + dataset + "file" + strconv.Itoa(i) + ".root"}
		sourceCatalog = append(sourceCatalog, rec)
	}
	copy(destinationCatalog[:], sourceCatalog)
	start := time.Now()
	records := core.CompareRecords(sourceCatalog, destinationCatalog)
	elapsed := time.Since(start)
	t.Log("For 10k it took", elapsed)
	if records != nil {
		t.Errorf("Incorrect Match for 1k files: %d", len(records))
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
