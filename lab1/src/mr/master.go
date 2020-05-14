package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Task struct
type Task struct {
	taskID    int      // Uniquely identifies a task
	taskType  int      // Including: Map, Reduce, NoTask
	filenames []string // Task related files

	taskState int       // Including: Idle, Working, Finished
	workerID  int       // ID of the worker who handles this task
	startTime time.Time // The time taskState turned from Idle to Working

	mapID    int // Uniquely identifies a map task
	reduceID int // Uniquely identifies a reduce task
}

// Master struct
type Master struct {
	// Your definitions here.
	nReduce int // Number of reduce tasks
	nMap    int // Numner of map tasks

	tasks     map[int]Task // Map task ID to tasks
	tasksLock sync.Mutex   // Lock for accessing tasks

	nextWorkerID int // Next worker ID to assign
	nextTaskID   int // Next task ID to assign

	intermediates []string // Intermediate files

	finishedMap     int        // Number of finished map tasks
	finishedMapLock sync.Mutex // Lock for accessing finishedMap

	finishedReduce     int        // Number of finished reduce tasks
	finishedReduceLock sync.Mutex // Lock for accessing finishedReduce
}

// Your code here -- RPC handlers for the worker to call.

// NextWorkerID will assign a id for a newly-come worker
func (m *Master) NextWorkerID() (id int) {
	id = m.nextWorkerID
	m.nextWorkerID++
	return
}

// NextTaskID will assign a id for a newly-come task
func (m *Master) NextTaskID() (id int) {
	id = m.nextTaskID
	m.nextTaskID++
	return
}

// GetWorkerID will be called by workers to get a worker id from the master
func (m *Master) GetWorkerID(
	args *GetWorkerIDArgs,
	reply *GetWorkerIDReply) error {

	reply.WorkerID = m.NextWorkerID()
	return nil
}

// GetTask will be called by workers to ask task from master.
func (m *Master) GetTask(
	args *GetTaskArgs,
	reply *GetTaskReply) error {

	reply.TaskType = NoTask

	m.tasksLock.Lock()
	defer m.tasksLock.Unlock()

	for taskID, task := range m.tasks {

		if task.taskState != Idle {
			continue
		}

		task.taskState = Working
		task.workerID = args.WorkerID
		task.startTime = time.Now()
		m.tasks[taskID] = task

		*reply = GetTaskReply{
			TaskID:    task.taskID,
			TaskType:  task.taskType,
			Filenames: task.filenames,
			MapID:     task.mapID,
			ReduceID:  task.reduceID,
			NReduce:   m.nReduce,
		}
		break
	}

	return nil
}

// NotifyTaskFinished will be called by workers to notify its task is finished
func (m *Master) NotifyTaskFinished(
	args *NotifyTaskFinishedArgs,
	reply *NotifyTaskFinishedReply) error {

	m.tasksLock.Lock()
	defer m.tasksLock.Unlock()

	task, ok := m.tasks[args.TaskID]
	if !ok {
		log.Fatalf("NotifyTaskFinished(): Unknown task id %v\n", args.TaskID)
	}

	if task.taskState == Finished {
		return nil
	}

	task.taskState = Finished
	m.tasks[args.TaskID] = task

	if task.taskType == Map {

		m.finishedMapLock.Lock()
		defer m.finishedMapLock.Unlock()
		m.finishedMap++

		for _, filename := range args.Filenames {
			spliter := func(r rune) bool { return r == '-' }
			fields := strings.FieldsFunc(filename, spliter)
			newFilename := fmt.Sprintf("mr-%v-%v", fields[1], fields[2])

			err := os.Rename(filename, newFilename)
			if err != nil {
				log.Fatalln(err)
			}

			m.intermediates = append(m.intermediates, newFilename)
		}

	} else if task.taskType == Reduce {

		m.finishedReduceLock.Lock()
		defer m.finishedReduceLock.Unlock()
		m.finishedReduce++

		filename := args.Filenames[0]

		spliter := func(r rune) bool { return r == '-' }
		fields := strings.FieldsFunc(filename, spliter)
		newFilename := fmt.Sprintf("mr-out-%v", fields[2])

		err := os.Rename(filename, newFilename)
		if err != nil {
			log.Fatalln(err)
		}
	}

	return nil
}

// Example for RPC
// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

// Done checks whether the entire job has finished.
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	// Your code here.
	m.finishedReduceLock.Lock()
	defer m.finishedReduceLock.Unlock()

	return m.finishedReduce == m.nReduce
}

// MakeMaster creates a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {

	// Your code here.

	m := Master{
		nReduce:            nReduce,
		nMap:               0,
		tasks:              make(map[int]Task),
		tasksLock:          sync.Mutex{},
		nextWorkerID:       0,
		nextTaskID:         0,
		intermediates:      make([]string, 0),
		finishedMap:        0,
		finishedMapLock:    sync.Mutex{},
		finishedReduce:     0,
		finishedReduceLock: sync.Mutex{},
	}

	for _, filename := range files {
		id := m.NextTaskID()
		task := Task{
			taskID:    id,
			taskType:  Map,
			filenames: []string{filename},
			taskState: Idle,
			workerID:  -1,
			startTime: time.Time{},
			mapID:     id,
			reduceID:  -1,
		}
		m.tasks[task.taskID] = task
		m.nMap++
	}

	go m.HandleTimeoutTask()
	go m.MergeIntermediates()

	m.server()
	return &m
}

// HandleTimeoutTask checks working tasks.
// If a working task does not finish in 10 seconds,
// it will be reset to idle and can be reassigned to
// other workers.
func (m *Master) HandleTimeoutTask() {
	for {
		func() {
			m.tasksLock.Lock()
			defer m.tasksLock.Unlock()

			for i := range m.tasks {
				if m.tasks[i].taskState != Working {
					continue
				}

				if time.Now().Sub(m.tasks[i].startTime) < 10*time.Second {
					continue
				}

				t := m.tasks[i]
				t.taskState = Idle
				m.tasks[i] = t
			}
		}()
		time.Sleep(time.Second)
	}
}

// MergeIntermediates first check whether all map tasks have finished.
// If so, it will merge intermediate files to nReduce files for the upcoming
// reduce tasks.
func (m *Master) MergeIntermediates() {
	for {
		isAllMapFinished := func() bool {
			m.finishedMapLock.Lock()
			defer m.finishedMapLock.Unlock()

			return m.finishedMap == m.nMap
		}()

		if isAllMapFinished {
			break
		}
		time.Sleep(time.Second)
	}

	reduceFiles := make(map[int][]string)
	for _, filename := range m.intermediates {
		spliter := func(r rune) bool { return r == '-' }
		fields := strings.FieldsFunc(filename, spliter)

		reduceID, err := strconv.Atoi(fields[2])
		if err != nil {
			log.Fatalln(err)
		}

		reduceFiles[reduceID] = append(reduceFiles[reduceID], filename)
	}

	m.tasksLock.Lock()
	defer m.tasksLock.Unlock()

	for i := 0; i < m.nReduce; i++ {
		task := Task{
			taskID:    m.NextTaskID(),
			taskType:  Reduce,
			filenames: reduceFiles[i],
			taskState: Idle,
			workerID:  -1,
			startTime: time.Time{},
			mapID:     -1,
			reduceID:  i,
		}
		m.tasks[task.taskID] = task
	}
}
