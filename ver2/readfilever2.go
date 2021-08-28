package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const _BLOCK_BUFFER = 64 * 1024
const _NUM_WORKER = 16

type rawData struct {
	startRow int
	rowNum   int
	str      [][]byte
}

type aggData struct {
	mapDate map[int]int
	mapName map[string]int
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
	wg         *sync.WaitGroup
	bp         *sync.Pool
	jobChannel chan rawData
	aggChannel chan aggData
	agg        aggData
}

func (w *worker) run() {
	defer w.wg.Done()
	split := splitter{
		numSplit: 9,
		arr:      make([][]byte, 0, 9),
	}
	for j := range w.jobChannel {
		check := j.startRow <= 43243
		actualRowNum := j.startRow
		commaSpace := []byte(", ")
		for i := 0; i < j.rowNum; i++ {

			split.split(j.str[i], '|')
			iDate, err := strconv.Atoi(string(split.arr[4][:6]))
			if err != nil {
				os.Exit(1)
			}
			w.agg.mapDate[iDate]++

			strFirstName := bytes.TrimSpace(split.arr[7])
			if len(strFirstName) > 0 {
				idx := bytes.Index(strFirstName, commaSpace)
				strFirstName = strFirstName[idx+2:]
				idx = bytes.IndexByte(strFirstName, ' ')
				if idx >= 0 {
					strFirstName = strFirstName[:idx]
				}

				if idx = bytes.IndexByte(strFirstName, ','); idx > 0 {
					strFirstName = strFirstName[:idx]
				}
			}
			w.agg.mapName[string(strFirstName)]++

			if check {
				switch actualRowNum {
				case 0, 432, 43243:
					fmt.Printf("Name: %s at index: %v\n", split.arr[7], actualRowNum)
				}
				actualRowNum++
			}
		}
		w.bp.Put(j.str)
	}
	w.aggChannel <- w.agg
}

type aggregator struct {
	wg         *sync.WaitGroup
	aggChannel chan aggData
	mapDate    map[int]int
	mapName    map[string]int
}

func (a *aggregator) run() {
	defer a.wg.Done()
	for d := range a.aggChannel {
		for k, v := range d.mapName {
			a.mapName[k] += v
		}
		for k, v := range d.mapDate {
			a.mapDate[k] += v
		}
	}
	maxNameCount := 0
	maxName := ""
	for k, v := range a.mapName {
		if maxNameCount < v {
			maxName = k
			maxNameCount = v
		}
	}

	for k, v := range a.mapDate {
		fmt.Printf("Donations per month and year: %v and donation count: %v\n", k, v)
	}
	fmt.Printf("The most common first name is: %s and it occurs: %d times.\n", maxName, maxNameCount)
}

func main() {
	start := time.Now()
	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	blockPool := &sync.Pool{New: func() interface{} {
		buff := make([][]byte, _BLOCK_BUFFER)
		for i := 0; i < _BLOCK_BUFFER; i++ {
			buff[i] = make([]byte, 0, 256)
		}
		return buff
	}}

	wgWorker := &sync.WaitGroup{}
	wgAggregator := &sync.WaitGroup{}

	chJob := make(chan rawData, _NUM_WORKER)
	chAgr := make(chan aggData, _NUM_WORKER)

	agr := aggregator{
		wg:         wgAggregator,
		aggChannel: chAgr,
		mapDate:    make(map[int]int),
		mapName:    make(map[string]int),
	}
	wgAggregator.Add(1)
	go agr.run()

	wgWorker.Add(_NUM_WORKER)
	for i := 0; i < _NUM_WORKER; i++ {
		w := worker{
			wg:         wgWorker,
			bp:         blockPool,
			jobChannel: chJob,
			aggChannel: chAgr,
			agg: aggData{
				mapDate: make(map[int]int),
				mapName: make(map[string]int),
			},
		}
		go w.run()
	}
	counter := 0
	ctr := 0
	startRow := counter
	bPool := blockPool.Get().([][]byte)

	_1MB := 1024 * 1024
	rbuf := make([]byte, _1MB) // 1MB buffer
	pos := 0
	posEnd := _1MB
	finished := false
BUFFERLOOP:
	for !finished {
		if pos > 0 {
			copy(rbuf, rbuf[pos:])
			pos = posEnd - pos
			posEnd = pos
		}
		n, err := file.Read(rbuf[pos:])
		if err == io.EOF { // finish reading
			finished = true
		} else {
			posEnd = n + pos
			pos = 0
		}

		for {
			if finished {
			}
			nl := bytes.IndexByte(rbuf[pos:posEnd], '\n')
			if nl < 0 {
				if !finished {
					continue BUFFERLOOP
				}
				bPool[ctr] = bPool[ctr][:0]
				bPool[ctr] = append(bPool[ctr], rbuf[pos:]...)

				poolSent := bPool
				chJob <- rawData{
					startRow: startRow,
					rowNum:   ctr,
					str:      poolSent,
				}
				break
			}

			if n == 0 {
				pos++
				continue BUFFERLOOP
			}
			bPool[ctr] = bPool[ctr][:0]
			bPool[ctr] = append(bPool[ctr], rbuf[pos:pos+nl]...)
			pos += nl + 1
			if pos > _1MB {
				pos = _1MB
			}
			ctr++
			counter++
			if ctr == _BLOCK_BUFFER {
				poolSent := bPool
				chJob <- rawData{
					startRow: startRow,
					rowNum:   ctr,
					str:      poolSent,
				}
				bPool = blockPool.Get().([][]byte)
				ctr = 0
				startRow = counter
			}
		}
	}
	fmt.Printf("total: %d, count finished in: %v\n", counter, time.Since(start))

	close(chJob)
	wgWorker.Wait()

	close(chAgr)
	wgAggregator.Wait()
	fmt.Printf("Program finished in: %v\n", time.Since(start))
}
