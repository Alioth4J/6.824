package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		argsForWork := Args{}
		task := Reply{}
		call("Master.AskForTask", &argsForWork, &task)
		switch task.Tpe {
		case 0:
			// Map
			content := getContentThroughFilename(task.Filename)
			kvs := mapf(task.Filename, content)
			args := Args{task.Id, kvs}
			reply := Reply{}
			call("Master.FinishMap", &args, &reply)
		case 1:
			// Reduce
			filenames := task.Filenames
			hashMap := make(map[string][]string)
			for _, filename := range filenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("Cannot open %v", filename)
				}
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Text()
					kv := strings.Split(line, " ")
					key := kv[0]
					value := kv[1]
					hashMap[key] = append(hashMap[key], value)
				}
			}
			var kvs []KeyValue
			for key, parts := range hashMap {
				value := reducef(key, parts)
				kv := KeyValue{key, value}
				kvs = append(kvs, kv)
			}
			args := Args{task.Id, kvs}
			reply := Reply{}
			call("Master.FinishReduce", &args, &reply)
		case 2:
			// Done
			break
		case 3:
			// Wait
			time.Sleep(1 * time.Second)
		default:
			panic(fmt.Errorf("Unknown task type: %v", task.Tpe))
		}
	}
}

func getContentThroughFilename(filename string) string {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args *Args, reply *Reply) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
