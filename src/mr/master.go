package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	Files   []string
	NReduce int

	Stage                    int   // 0 -> Map, 1 -> Reduce, 2 -> Done
	MapStatus                []int // 0 -> waiting, 1 -> executing, 2 -> done
	MapStartTime             []time.Time
	CompletedMapTaskCount    int
	ReduceStatus             []int // 0 -> waiting, 1 -> executing, 2 -> done
	ReduceStartTime          []time.Time
	CompletedReduceTaskCount int

	ReduceWorks [][]string

	lock sync.Mutex
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		Files:                    files,
		NReduce:                  nReduce,
		Stage:                    0,
		MapStatus:                make([]int, len(files)),
		MapStartTime:             make([]time.Time, len(files)),
		CompletedMapTaskCount:    0,
		ReduceStatus:             make([]int, nReduce),
		ReduceStartTime:          make([]time.Time, nReduce),
		CompletedReduceTaskCount: 0,
		ReduceWorks:              make([][]string, nReduce),
	}
	m.server()
	return &m
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) AskForWork(args *Args, task *Reply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	switch m.Stage {
	case 0:
		// Map
		for i, status := range m.MapStatus {
			if status == 0 {
				task.Id = i
				task.Tpe = 0
				task.Filename = m.Files[i]
				m.MapStatus[i] = 1
				m.MapStartTime[i] = time.Now()
				return nil
			} else if status == 1 {
				if time.Now().After(m.MapStartTime[i].Add(10 * time.Second)) {
					task.Id = i
					task.Tpe = 0
					task.Filename = m.Files[i]
					m.MapStatus[i] = 1
					m.MapStartTime[i] = time.Now()
					return nil
				} else {
					continue
				}
			} else if status == 2 {
				continue
			}
		}
		// wait
		task.Tpe = 3
	case 1:
		// Reduce
		for i, status := range m.ReduceStatus {
			if status == 0 {
				task.Id = i
				task.Tpe = 1
				task.Filenames = m.ReduceWorks[i]

				m.ReduceStatus[i] = 1
				m.ReduceStartTime[i] = time.Now()

				return nil
			} else if status == 1 {
				if time.Now().After(m.ReduceStartTime[i].Add(10 * time.Second)) {
					task.Id = i
					task.Tpe = 1
					task.Filenames = m.ReduceWorks[i]

					m.ReduceStatus[i] = 1
					m.ReduceStartTime[i] = time.Now()

					return nil
				} else {
					continue
				}
			} else if status == 2 {
				continue
			}
		}
	case 2:
		// Done
		task.Tpe = 2
	}

	return nil
}

func (m *Master) FinishMap(args *Args, reply *Reply) error {
	if m.MapStatus[args.Id] == 1 {
		m.lock.Lock()
		if m.MapStatus[args.Id] == 1 {
			// store to "mr-x-y" file
			groups := make(map[int][]KeyValue)
			for _, kv := range args.Kvs {
				y := ihash(kv.Key) % m.NReduce
				groups[y] = append(groups[y], kv)
			}

			for y, kvs := range groups {
				filename := "mr-" + strconv.Itoa(args.Id) + "-" + strconv.Itoa(y)
				// about the first param, can't use "" while using Silverblue
				tempFile, err := ioutil.TempFile(".", "temp-")
				if err != nil {
					return err
				}

				for _, kv := range kvs {
					tempFile.WriteString(kv.Key + " " + kv.Value + "\n")
				}
				tempFile.Close()

				os.Rename(tempFile.Name(), filename)
				m.ReduceWorks[y] = append(m.ReduceWorks[y], filename)
			}

			// change the status of the work
			m.MapStatus[args.Id] = 2
			m.CompletedMapTaskCount++

			// change the stage of the master
			if m.CompletedMapTaskCount == len(m.Files) {
				m.Stage = 1
			}
		}
		m.lock.Unlock()
	}

	return nil
}

func (m *Master) FinishReduce(args *Args, reply *Reply) error {
	if m.ReduceStatus[args.Id] == 1 {
		m.lock.Lock()
		if m.ReduceStatus[args.Id] == 1 {
			hashMap := make(map[string]string)
			var arrayList []string
			for _, kv := range args.Kvs {
				hashMap[kv.Key] = kv.Value
				arrayList = append(arrayList, kv.Key)
			}
			sort.Strings(arrayList)

			filename := "mr-out-" + strconv.Itoa(args.Id)
			file, err := os.Create(filename)
			if err != nil {
				return err
			}
			defer file.Close()
			for _, key := range arrayList {
				fmt.Fprintf(file, "%v %v\n", key, hashMap[key])
			}

			// change the status of the work
			m.ReduceStatus[args.Id] = 2
			m.CompletedReduceTaskCount++

			// change the stage of Master
			if m.CompletedReduceTaskCount == m.NReduce {
				m.Stage = 2
			}
		}
		m.lock.Unlock()
	}
	return nil
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	if m.Stage == 2 {
		return true
	}
	return false
}
