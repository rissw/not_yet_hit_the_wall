package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

const (
	_FILE_BUFFER_SIZE    = 64 * 1024
	_NEW_LINE            = '\n'
	_LAST_PRINTED_RECORD = 43243
	_SEPARATOR           = byte('|')
	_SPACE               = byte(' ')
	_COMMA               = byte(',')
)

var _COMMA_SPACE = []byte(", ")

type aggregateData struct {
	mapName map[string]int
	mapDate map[int]int
}

type worker struct {
	wg              *sync.WaitGroup
	rc              int
	data            []byte
	chMainOffer     chan *[]byte
	chStartRC       chan int
	chAggregateData chan aggregateData
	ag              aggregateData
}

func (w *worker) resetData() *[]byte {
	w.data = w.data[:0]
	return &w.data
}

func (w *worker) run() {
	defer w.wg.Done()
	split := splitter{
		numSplit: 8,
		arr:      make([][]byte, 0, 8),
	}

	var rest []byte
	var processedData []byte

MAINLOOP:
	for {
		select {
		case <-w.chStartRC:
			break MAINLOOP
		case w.chMainOffer <- w.resetData():
		}

		rc, ok := <-w.chStartRC
		if !ok {
			break
		}
		checkPrint := (rc > -1) && rc < _LAST_PRINTED_RECORD
		rest = w.data
		stop := false
		for !stop {
			idx := bytes.IndexByte(rest, _NEW_LINE)
			if idx < 0 {
				stop = true
				if len(rest) > 0 {
					processedData = rest
				}
			} else {
				processedData = rest[:idx]
				rest = rest[idx+1:]
			}
			split.split(processedData, _SEPARATOR)
			iDate := bytesToInt(split.arr[4][:6])
			if iDate < 0 {
				os.Exit(1)
			}
			w.ag.mapDate[iDate]++

			strFirstName := extractFirstName(split.arr[7])
			w.ag.mapName[string(strFirstName)]++

			if checkPrint {
				switch rc {
				case 0, 432:
					fmt.Printf("Index record %d is '%s'\n", rc, split.arr[7])
				case 43243:
					fmt.Printf("Index record %d is '%s'\n", rc, split.arr[7])
					checkPrint = false
				}
				rc++

			}
		}
	}
	w.chAggregateData <- w.ag
}

func main() {
	start := time.Now()

	_NUM_WORKER := runtime.GOMAXPROCS(0)

	mapName := make(map[string]int)
	mapDate := make(map[int]int)

	file, err := os.Open(os.Args[1])
	if err != nil {
		os.Exit(1)
	}
	defer file.Close()

	rbuf := make([]byte, _FILE_BUFFER_SIZE)
	chStartRC := make(chan int, _NUM_WORKER)
	chAggregateData := make(chan aggregateData, 2*_NUM_WORKER)
	chMainOffer := make(chan *[]byte, 2*_NUM_WORKER)
	wg := &sync.WaitGroup{}
	wg.Add(_NUM_WORKER)

	for i := 0; i < _NUM_WORKER; i++ {
		w := &worker{
			wg:              wg,
			chMainOffer:     chMainOffer,
			chStartRC:       chStartRC,
			chAggregateData: chAggregateData,
			ag: aggregateData{
				mapName: make(map[string]int),
				mapDate: make(map[int]int),
			},
			data: make([]byte, 0, _FILE_BUFFER_SIZE),
		}
		go w.run()
	}

	endReading := false
	pos := 0 // position of the rbuf
	recordCount := 0
	var bf *[]byte
	for !endReading {
		if endReading {
			break
		}
		bf = <-chMainOffer
		n, err := file.Read(rbuf[pos:])
		if err == io.EOF {
			endReading = true
			if pos > 0 {
				*bf = append(*bf, rbuf[:pos-1]...)
				chStartRC <- recordCount
			}
			break
		}

		// find last '\n'
		lastIndex := bytes.LastIndexByte(rbuf[:pos+n], _NEW_LINE)
		rc := -1
		if recordCount < _LAST_PRINTED_RECORD {
			rc = countNewLine(rbuf, lastIndex)
			rcDummy := recordCount
			recordCount += rc
			rc = rcDummy
		}
		*bf = append(*bf, rbuf[:lastIndex]...)
		if lastIndex < _FILE_BUFFER_SIZE {
			copy(rbuf, rbuf[lastIndex+1:pos+n])
			pos = pos + n - (lastIndex + 1)
		} else {
			pos = 0
		}
		chStartRC <- rc
	}

	close(chStartRC)
	wg.Wait()

	close(chAggregateData)
	count := 0
	for a := range chAggregateData {
		for k, v := range a.mapDate {
			mapDate[k] += v
			count += v
		}
		for k, v := range a.mapName {
			mapName[k] += v
		}
	}

	fmt.Printf("Total record: %d counted in %v\n", count, time.Since(start))

	maxName := ""
	maxNameCount := 0
	for k, v := range mapName {
		if maxNameCount < v {
			maxNameCount = v
			maxName = k
		}
	}

	for k, v := range mapDate {
		fmt.Printf("Donations per month and year: %v and donation count: %v\n", k, v)
	}

	fmt.Printf("Name '%s' has the most count, %d times\n", maxName, maxNameCount)

	fmt.Printf("Finished in %v\n", time.Since(start))
}

// -------------<< small functions >>-----------------

type splitter struct {
	numSplit int
	arr      [][]byte
}

func (sp *splitter) split(s []byte, sep byte) {
	sp.arr = sp.arr[:0]
	i := 0
	for i < sp.numSplit {
		m := bytes.IndexByte(s, sep)
		if m < 0 {
			break
		}
		sp.arr = append(sp.arr, s[:m])
		s = s[m+1:]
		i++
	}
}

func bytesToInt(b []byte) int {
	val := 0
	for i := 0; i < len(b); i++ {
		if b[i] > '9' || b[i] < '0' {
			return -1
		}
		val = val*10 + int(b[i]-'0')
	}
	return val
}

func trimSpaces(b []byte) []byte {
	i := 0
	for i < len(b) {
		if b[i] != _SPACE {
			b = b[i:]
			break
		}
		i++
	}

	return b
}

func extractFirstName(b []byte) []byte {
	b = trimSpaces(b)
	if len(b) == 0 {
		return b
	}

	b = b[bytes.Index(b, _COMMA_SPACE)+2:]

	for i := 0; i < len(b); i++ {
		switch b[i] {
		case _COMMA, _SPACE:
			b = b[:i]
			break
		}
	}

	return b
}

func countNewLine(b []byte, lastIndex int) (count int) {
	for i := 0; i <= lastIndex; i++ {
		if b[i] == _NEW_LINE {
			count++
		}
	}
	return
}
