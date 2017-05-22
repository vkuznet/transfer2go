// Writing files in Go follows similar patterns to the
// ones we saw earlier for reading.

package main

import (
	"os"
	"strconv"
)

func main() {

	f, err := os.OpenFile("data.sql", os.O_APPEND|os.O_WRONLY, 0600)

	if err != nil {
		panic(err)
	}

	defer f.Close()

	for i := 1; i <= 1000; i = i + 3 {
		str := strconv.Itoa(i)
		_, err := f.WriteString("insert into BLOCKS values (" + str + ", \"" + str + "\"" + ");\n")
		_, err = f.WriteString("insert into datasets values (" + str + ", \"" + str + "\"" + ");\n")
		_, err = f.WriteString("insert into FILES values (" + str + ", \"" + str + "\"" + ", /path/file3.root, " + str + ", " + str + ",1" + ", hash" + ", 123" + ", 123" + ");\n")
		if err != nil {
			panic(err)
		}
	}

}
