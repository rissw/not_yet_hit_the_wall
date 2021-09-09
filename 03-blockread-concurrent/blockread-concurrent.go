package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

const (
	_NEW_LINE         = byte('\n')
	_FILE_BUFFER_SIZE = 256 * 1024
)

type worker struct {
	wg       *sync.WaitGroup
	chIn     chan *[]byte
	chReturn chan *[]byte
	chOut    chan int
}

func (w *worker) run() {
	defer w.wg.Done()

	for j := range w.chIn {
		count := 0
		for i := 0; i < len(*j); i++ {
			if (*j)[i] == _NEW_LINE {
				count++
			}
		}
		w.chOut <- count
		w.chReturn <- j
	}
}

type aggregator struct {
	wg      *sync.WaitGroup
	chCount chan int
	result  *int
}

func (a *aggregator) run() {
	defer a.wg.Done()
	for j := range a.chCount {
		*(a.result) += j
	}
}

func main() {
	start := time.Now()
	numWorkerMAX := runtime.GOMAXPROCS(0)
	numWorker := numWorkerMAX - 1
	if numWorkerMAX < 8 {
		numWorker = numWorkerMAX
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	totalCount := 0

	chBuf := make(chan *[]byte, 2*numWorkerMAX)
	chProcess := make(chan *[]byte, 2*numWorkerMAX)
	chAggregate := make(chan int, 2*numWorkerMAX)

	for i := 0; i < 2*numWorkerMAX; i++ {
		buffy := make([]byte, _FILE_BUFFER_SIZE)
		chBuf <- &buffy
	}

	wg := &sync.WaitGroup{}
	wgAggregate := &sync.WaitGroup{}

	wgAggregate.Add(1)
	ag := &aggregator{
		wg:      wgAggregate,
		chCount: chAggregate,
		result:  &totalCount,
	}
	go ag.run()

	wg.Add(numWorker)
	for i := 0; i < numWorker; i++ {
		w := &worker{
			wg:       wg,
			chIn:     chProcess,
			chReturn: chBuf,
			chOut:    chAggregate,
		}

		go w.run()
	}

	for {
		bf := <-chBuf
		n, err := file.Read(*bf)
		if err == io.EOF {
			break
		}

		if n != _FILE_BUFFER_SIZE {
			*bf = (*bf)[:n]
		}

		chProcess <- bf

	}

	close(chProcess)
	wg.Wait()

	close(chAggregate)
	wgAggregate.Wait()

	close(chBuf)

	fmt.Println("Number of line:", totalCount)
	fmt.Printf("Finished in %v\n", time.Since(start))
}
