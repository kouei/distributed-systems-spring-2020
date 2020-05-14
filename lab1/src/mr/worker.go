package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// KeyValue struct
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// HandleMap handles map jobs
func HandleMap(
	workerID int,
	filenames []string,
	mapID int,
	nReduce int,
	mapf func(string, string) []KeyValue) (intermediates []string) {

	kva := make([]KeyValue, 0)
	for _, filename := range filenames {
		func() {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalln(err)
			}
			defer file.Close()

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalln(err)
			}

			kvCur := mapf(filename, string(content))
			kva = append(kva, kvCur...)
		}()
	}

	intermediateFilenames := make(map[string]bool)
	for _, kv := range kva {
		func() {
			reduceID := ihash(kv.Key) % nReduce
			filename := fmt.Sprintf("mr-%v-%v-%v", mapID, reduceID, workerID)
			intermediateFilenames[filename] = true

			file, err := os.OpenFile(filename,
				os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalln(err)
			}
			defer file.Close()

			fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
		}()
	}

	for k := range intermediateFilenames {
		intermediates = append(intermediates, k)
	}
	return
}

// ByKey is used for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// HandleReduce handles reduce jobs
func HandleReduce(
	workerID int,
	reduceID int,
	filenames []string,
	reducef func(string, []string) string) (outputFilename string) {

	kva := make([]KeyValue, 0)
	for _, filename := range filenames {
		func() {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalln(err)
			}
			defer file.Close()

			for {
				k := ""
				v := ""
				nRead, err := fmt.Fscanf(file, "%s %s", &k, &v)
				if nRead != 2 || err != nil {
					break
				}
				kva = append(kva, KeyValue{Key: k, Value: v})
			}
		}()
	}

	sort.Sort(ByKey(kva))

	outputFilename = fmt.Sprintf("mr-out-%v-%v", reduceID, workerID)
	file, err := os.Create(outputFilename)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

		i = j
	}
	return
}

// Worker handles worker's job
// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	workerID, ok := CallGetWorkerID()

	if !ok {
		log.Fatalln("CallGetWorkerID() failed")
	}

	for {

		taskID, taskType, filenames, mapID, reduceID, nReduce, ok :=
			CallGetTask(workerID)

		if !ok {
			log.Fatalf("Worker %d: CallGetTask() failed\n", workerID)
		}

		if taskType == Map {
			intermediates := HandleMap(
				workerID, filenames, mapID, nReduce, mapf)

			ok := CallNotifyTaskFinished(taskID, intermediates)
			if !ok {
				log.Fatalf("Worker %d: CallNotifyTaskFinished() failed\n", workerID)
			}

		} else if taskType == Reduce {
			outputFilename := HandleReduce(
				workerID, reduceID, filenames, reducef)
			ok := CallNotifyTaskFinished(taskID, []string{outputFilename})
			if !ok {
				log.Fatalf("Worker %d: CallNotifyTaskFinished() failed\n", workerID)
			}

		} else if taskType == NoTask {
			time.Sleep(time.Second)
		} else {
			log.Fatalf("Unknown task type: %v\n", taskType)
		}
	}
}

// CallExample shows how to communicate with master through RPC
// example function to show how to make an RPC call to the master.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// CallGetWorkerID calls the GetWorkerID() through RPC
func CallGetWorkerID() (workerID int, ok bool) {
	args := GetWorkerIDArgs{}
	reply := GetWorkerIDReply{}

	ok = call("Master.GetWorkerID", &args, &reply)
	workerID = reply.WorkerID

	return
}

// CallGetTask calls the GetTask() through RPC
func CallGetTask(workerID int) (
	taskID int,
	taskType int,
	filenames []string,
	mapID int,
	reduceID int,
	nReduce int,
	ok bool) {

	args := GetTaskArgs{WorkerID: workerID}
	reply := GetTaskReply{}

	// send the RPC request, wait for the reply.
	ok = call("Master.GetTask", &args, &reply)

	taskID = reply.TaskID
	taskType = reply.TaskType
	filenames = reply.Filenames
	mapID = reply.MapID
	reduceID = reply.ReduceID
	nReduce = reply.NReduce
	return
}

// CallNotifyTaskFinished calls the NotifyTaskFinished() through RPC
func CallNotifyTaskFinished(taskID int, filenames []string) (ok bool) {
	args := NotifyTaskFinishedArgs{
		TaskID:    taskID,
		Filenames: filenames,
	}
	reply := NotifyTaskFinishedReply{}

	ok = call("Master.NotifyTaskFinished", &args, &reply)
	return
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
