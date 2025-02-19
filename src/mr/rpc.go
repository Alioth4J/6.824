package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type Args struct {
	Id  int
	Kvs []KeyValue
}

// This is the task.
type Reply struct {
	Id  int // x or y
	Tpe int // 0 -> Map, 1 -> Reduce, 2 -> Done, 3 -> Wait

	Filename  string   // only used in Map
	Filenames []string // only used in Reduce
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
