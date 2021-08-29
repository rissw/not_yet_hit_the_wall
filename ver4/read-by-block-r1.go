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

type rawData struct {
	startRC int //recordCount, if -1 means ignore record number
	data    []byte
}

type aggregateData struct {
	mapName map[string]int
	mapDate map[int]int
}

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

	// i = len(b) - 1
	// for i > 0 {
	// 	if b[i] != _SPACE {
	// 		b = b[:i+1]
	// 		break
	// 	}
	// }

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

type worker struct {
	freePool        *sync.Pool
	wg              *sync.WaitGroup
	chRawData       chan rawData
	chAggregateData chan aggregateData
	ag              aggregateData
}

func (w *worker) run() {
	defer w.wg.Done()
	split := splitter{
		numSplit: 8,
		arr:      make([][]byte, 0, 8),
	}
	for j := range w.chRawData {
		checkPrint := (j.startRC > -1) && j.startRC < _LAST_PRINTED_RECORD
		rc := j.startRC
		rest := j.data
		var processedData []byte
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
		w.freePool.Put(j.data)
	}
	w.chAggregateData <- w.ag
}

func countNewLine(b []byte, lastIndex int) (count int) {
	for i := 0; i <= lastIndex; i++ {
		if b[i] == _NEW_LINE {
			count++
		}
	}
	return
}

func main() {
	start := time.Now()
	numWorker := runtime.GOMAXPROCS(0)

	mapName := make(map[string]int)
	mapDate := make(map[int]int)

	file, err := os.Open(os.Args[1])
	if err != nil {
		os.Exit(1)
	}
	defer file.Close()

	poolReadBlock := &sync.Pool{New: func() interface{} {
		buf := make([]byte, 0, _FILE_BUFFER_SIZE)
		return buf
	}}

	rbuf := make([]byte, _FILE_BUFFER_SIZE)
	chRawData := make(chan rawData, 2*numWorker)
	chAggregateData := make(chan aggregateData, numWorker)

	wgWorker := &sync.WaitGroup{}

	wgWorker.Add(numWorker)

	for i := 0; i < numWorker; i++ {
		w := &worker{
			wg:              wgWorker,
			freePool:        poolReadBlock,
			chRawData:       chRawData,
			chAggregateData: chAggregateData,
			ag: aggregateData{
				mapName: make(map[string]int),
				mapDate: make(map[int]int),
			},
		}
		go w.run()
	}
	endReading := false
	pos := 0 // position of the rbuf
	recordCount := 0
	for !endReading {
		n, err := file.Read(rbuf[pos:])
		if err == io.EOF {
			endReading = true
			if pos > 0 {
				bf := poolReadBlock.Get().([]byte)[:0]
				bf = append(bf, rbuf[:pos-1]...)
				chRawData <- rawData{
					startRC: recordCount,
					data:    bf,
				}
			}
			break
		}

		bf := poolReadBlock.Get().([]byte)[:0]
		// find last '\n'
		lastIndex := bytes.LastIndexByte(rbuf[:pos+n], _NEW_LINE)
		rc := -1
		if recordCount < _LAST_PRINTED_RECORD {
			rc = countNewLine(rbuf, lastIndex)
			rcDummy := recordCount
			recordCount += rc
			rc = rcDummy
		}
		bf = append(bf, rbuf[:lastIndex]...)
		if lastIndex < _FILE_BUFFER_SIZE {
			copy(rbuf, rbuf[lastIndex+1:])
			pos = pos + n - (lastIndex + 1)
		} else {
			pos = 0
		}
		chRawData <- rawData{
			startRC: rc,
			data:    bf,
		}
	}

	close(chRawData)
	wgWorker.Wait()

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
