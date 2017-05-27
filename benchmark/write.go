// Writing files in Go follows similar patterns to the
// ones we saw earlier for reading.

package main

import (
	"math/rand"
	"os"
	"strconv"
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
		dataset := String(5)
		f.WriteString("insert into DATASETS values (" + strconv.Itoa(i) + ", " + "\"" + dataset + "\"" + ");\n")
		for j := 1; j <= 100; j++ {
			block := String(4)
			f.WriteString("insert into BLOCKS values (" + strconv.Itoa(i*1000+j) + ", " + "\"" + block + "\"" + ");\n")
			for k := 1; k <= 100; k++ {
				file := dataset + "-" + block + "-" + String(5) + strconv.Itoa(i) + ".root"
				id := i*1000000 + j*1000 + k
				f.WriteString("insert into FILES values (" + strconv.Itoa(id) + ", " + "\"" + file + "\"" + ", \"" + "/path/file3.root" + "\", " + strconv.Itoa(j) + ", " + strconv.Itoa(i) + ", 10, " + "\"" + "hash" + "\"" + ", 123" + ", 123" + ");\n")
			}
		}
	}
}

// Get max length of random string
func String(length int) string {
	return StringWithCharset(length, charset)
}

// Function to generate random string
func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
