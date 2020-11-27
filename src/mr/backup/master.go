package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strings"
import "fmt"
import "sync"

type Master struct {
	fileWorker map[string] workerStatus
	mapDone bool
	reduceWorker map[int] workerStatus
	reduceDone bool
	workerID int
        nReduce int
        mapWorkerID []int	
        mux sync.Mutex
}
//Status = -1 unassigned, status = 0 assigned but not completed, status = 1 completed
type workerStatus struct {
	id int
 	status int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) AskWork(args *ExampleArgs, reply *ExampleReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.mapDone != true {
		for file, worker := range m.fileWorker {
			if worker.status == -1 {
				reply.FileName = file
                		reply.NReduce =  m.nReduce
                		reply.Id = m.workerID
				reply.WorkType = "map"
				m.fileWorker[file] = workerStatus{m.workerID, 0}
	 			m.workerID = m.workerID + 1	
				break
	 		}
      		}
	} else if m.reduceDone != true {
		for bucket, worker := range m.reduceWorker {
			if worker.status == -1 {
				reply.Bucket = bucket
				reply.NReduce = m.nReduce
				reply.Id = m.workerID
				reply.WorkType = "reduce"
				reply.MapWorkerID = m.mapWorkerID
				m.reduceWorker[bucket] = workerStatus{m.workerID, 0}
				m.workerID = m.workerID + 1
                                //fmt.Println("Starting reduce task for bucket:", bucket)
				break
			}
		}
 	} else {

			reply.WorkType = "Done"
			fmt.Println("All Done")
		}
				
	return nil
}

func (m *Master) UpdateStatusMap(args *ExampleArgs, reply *ExampleReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()
        worker := m.fileWorker[args.FileName]  	
        worker.status = 1
	m.fileWorker[args.FileName] = worker
	m.mapWorkerID = append(m.mapWorkerID, args.Id)
	temp := true
	for _, worker := range m.fileWorker {
		if worker.status != 1 {
			temp = false
			break	
		}
	}
	m.mapDone = temp
	if m.mapDone == true {
		fmt.Println("Maptasks Completed")
	}
   return nil
 }

func (m *Master) UpdateStatusReduce(args *ExampleArgs, reply *ExampleReply) error {
	m.mux.Lock()
	defer m.mux.Unlock()
        worker := m.reduceWorker[args.Bucket]  	
        worker.status = 1
	m.reduceWorker[args.Bucket] = worker
	temp :=true
	for _, worker := range m.reduceWorker {
		if worker.status != 1 {
			temp = false
			break	
		}
	}
	m.reduceDone = temp
	if m.reduceDone == true {
		fmt.Println("All reducetasks Completed")
	}
   return nil
 }
//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	m.mux.Lock()
        defer m.mux.Unlock()
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


//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
        m.mux.Lock()
	defer m.mux.Unlock()
	if m.mapDone && m.reduceDone {
	ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{workerID: 0, nReduce: nReduce}
	m.fileWorker = make(map[string] workerStatus)
	m.reduceWorker = make(map[int] workerStatus)
        m.mapWorkerID = make([]int,0)
	m.mapDone = false
	m.reduceDone = false

	for _, filename:= range files {
	filename := strings.Join([]string{"/Users/kalpeshpatel/mit-class/src/main",filename},"/")
	m.fileWorker[filename] = workerStatus{id: -1,status: -1}
        fmt.Println("Full filename: ", filename)
	}
	for i := 0; i < nReduce; i++ {
	m.reduceWorker[i] = workerStatus{id: -1, status: -1}
	} 
	// Your code here.
	m.server()
	return &m
}
