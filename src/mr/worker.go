package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
//import "io"
import s "strings"
import "strconv"
import "encoding/json"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
        //Add while loop here to call until done
 
   reply := CallExample("Master.AskWork",ExampleArgs{})
   for reply.WorkType != "Done" {
	 //reply := CallExample("Master.AskWork",ExampleArgs{})
	if reply.WorkType == "map" {
 		fmt.Printf("reply.FileName %v\n", reply.FileName)
	  	fmt.Printf("reply.Id %v\n",reply.Id)
		 fmt.Printf("reply.WorkType %v\n",reply.WorkType)
           	writeMapFiles(mapf,reply) 
           	args := ExampleArgs{}
	   	args.Id = reply.Id
	   	args.FileName = reply.FileName
           	CallExample("Master.UpdateStatusMap",args)
	}
	if reply.WorkType == "reduce" {

	   reduceTask(reducef,reply)	
           args := ExampleArgs{}
	   args.Id = reply.Id
	   args.Bucket = reply.Bucket
           CallExample("Master.UpdateStatusReduce", args)
	} 
   reply = CallExample("Master.AskWork",ExampleArgs{})
 } 
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func writeMapFiles(mapf func(string, string) []KeyValue, params ExampleReply) {
	file, err := os.Open(params.FileName)
                if err != nil {
                        log.Fatalf("cannot open %v", params.FileName)
		}
       content, err := ioutil.ReadAll(file)
              if err != nil {
                  log.Fatalf("cannot read %v", params.FileName)
                }
       file.Close()
       mapStore := mapf(params.FileName, string(content))
	var filename = make([]string,params.NReduce)
        var tmpfile = make([]*os.File, params.NReduce)
	var jsonEnc = make([]*json.Encoder,params.NReduce)
	for i := 0; i < params.NReduce; i++ {
		filename[i] = s.Join([]string{"mr",strconv.Itoa(params.Id),strconv.Itoa(i)},"-")
		filedes,err := ioutil.TempFile("", filename[i])
		if err != nil {
		log.Fatal(err)
	   	}
		tmpfile[i] = filedes 
		jsonEnc[i] = json.NewEncoder(filedes)
		defer os.Remove(tmpfile[i].Name())
	}
	for _,item := range mapStore {
		reduceNo := ihash(item.Key) % params.NReduce
		err := jsonEnc[reduceNo].Encode(item)
		if err != nil {
		log.Fatal(err)
	}
     }
	for i := 0; i < params.NReduce; i++ {
		err := os.Rename(tmpfile[i].Name(),s.Join([]string{"/Users/kalpeshpatel/mit-class/src/main/mr-tmp",filename[i]},"/"))
		if err != nil {
		log.Fatal(err)
		}
		if err := tmpfile[i].Close(); err != nil {
		log.Fatal(err)
		}
	}	
}

func reduceTask(reducef func(string, []string) string, params ExampleReply) {
//Reduce is to work on all files of the form mr-workerID-reduceID
//It needs to iterate through all workerID
        mapStore := []KeyValue{}
	length := len(params.MapWorkerID)
	for i:= 0;i < length; i++ {
		filename := s.Join([]string{"/Users/kalpeshpatel/mit-class/src/main/mr-tmp/mr",strconv.Itoa(params.MapWorkerID[i]),strconv.Itoa(params.Bucket)},"-")
		filedes,err := os.Open(filename)
                defer filedes.Close()
		check(err)		
		jsonDecoder := json.NewDecoder(filedes)
                for {
  			kv := KeyValue{}
			err := jsonDecoder.Decode(&kv)
			if err != nil {
				break
			}	
			mapStore = append(mapStore,kv)
		}
	}

	sort.Sort(ByKey(mapStore))
	oname := s.Join([]string{"/Users/kalpeshpatel/mit-class/src/main/mr-tmp/mr","out",strconv.Itoa(params.Bucket)},"-")
        ofile, _ := os.Create(oname)

        //
        // call Reduce on each distinct key in intermediate[],
        // and print the result to mr-out-0.
        //
	//fmt.Println("Mapstore:", mapStore)
        i := 0
        for i < len(mapStore) {
                j := i + 1
                for j < len(mapStore) && mapStore[j].Key == mapStore[i].Key {
                        j++
                }
                values := []string{}
                for k := i; k < j; k++ {
                        values = append(values, mapStore[k].Value)
                }
                output := reducef(mapStore[i].Key, values)

                // this is the correct format for each line of Reduce output.
                fmt.Fprintf(ofile, "%v %v\n", mapStore[i].Key, output)

                i = j
        }

        ofile.Close()

}
func CallExample(callName string, args ExampleArgs) ExampleReply {

	// declare a reply structure.
	reply := ExampleReply{}
	// send the RPC request, wait for the reply.
	call(callName, &args, &reply)
        return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
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

func check(e error) {
    if e != nil {
        panic(e)
    }
}
