package main

import (
	"fmt"
	"os"
	"time"
)

const (
	_NEW_LINE = byte('\n')
)

func main() {
	start := time.Now()

	file, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	lineCount := 0
	for i := 0; i < len(file); i++ {
		if file[i] == _NEW_LINE {
			lineCount++
		}
	}

	fmt.Println("Number of line:", lineCount)
	fmt.Printf("Finished in %v\n", time.Since(start))
}
