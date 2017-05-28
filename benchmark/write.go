// Writing files in Go follows similar patterns to the
// ones we saw earlier for reading.

package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func main() {

	f, err := os.OpenFile("data.sql", os.O_APPEND|os.O_WRONLY, 0600)

	if err != nil {
		panic(err)
	}

	defer f.Close()

	for i := 1; i <= 100; i++ {
		datasetID := i
		dataset := "/" + String(3) + "/" + String(3) + "/" + String(3)
		putDataset := fmt.Sprintf("insert into DATASETS values (%d, %s); \n", datasetID, "\""+dataset+"\"")
		f.WriteString(putDataset)

		for j := 1; j <= 100; j++ {
			blockHash := String(4)
			block := fmt.Sprintf("%s#%s", dataset, blockHash)
			blockID := i*1000 + j
			putBlock := fmt.Sprintf("insert into BLOCKS values (%d, %s, %d); \n", blockID, "\""+block+"\"", datasetID)
			f.WriteString(putBlock)

			for k := 1; k <= 100; k++ {
				lfn := dataset + "-" + block + "-" + String(5) + ".root"
				pfn := "/path/file3.root"
				id := i*1000000 + j*1000 + k
				putFile := fmt.Sprintf("insert into FILES values(%d, %s, %s, %d, %d, %d, %s, %d, %d); \n", id, "\""+lfn+"\"", "\""+pfn+"\"", blockID, datasetID, 10, "\"hash\"", 123, 123)
				f.WriteString(putFile)
			}

		}

	}

}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
