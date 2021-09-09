package main

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

func main() {
	start := time.Now()
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	fd := int(f.Fd())

	stat, err := f.Stat()
	if err != nil {
		panic(err)
	}
	size := int(stat.Size())

	b, err := syscall.Mmap(fd, 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	count := 0
	for i := 0; i < len(b); i++ {
		if b[i] == '\n' {
			count++
		}
	}

	defer syscall.Munmap(b)
	fmt.Println("Length:", len(b), ", Lines:", count)
	fmt.Printf("Time: %v\n", time.Since(start))
}
