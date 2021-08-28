package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	_FILE_BUFFER_SIZE    = 512 * 1024
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
		numSplit: 9,
		arr:      make([][]byte, 0, 9),
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
			iDate, err := strconv.Atoi(string(split.arr[4][:6]))
			if err != nil {
				os.Exit(1)
			}
			w.ag.mapDate[iDate]++

			strFirstName := bytes.TrimSpace(split.arr[7])
			if len(strFirstName) > 0 {
				idx := bytes.Index(strFirstName, _COMMA_SPACE)
				strFirstName = strFirstName[idx+2:]
				idx = bytes.IndexByte(strFirstName, _SPACE)
				if idx >= 0 {
					strFirstName = strFirstName[:idx]
				}

				if idx = bytes.IndexByte(strFirstName, _COMMA); idx > 0 {
					strFirstName = strFirstName[:idx]
				}
			}
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

	numWorker := runtime.GOMAXPROCS(0)
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
