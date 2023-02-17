package mr

import (
	"io/fs"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type workerStatus int
type CurrentTaskType int
type TaskStatus int

const taskExpirationTime = 10 * time.Second

const (
	idle workerStatus = iota
	dead
	working
)

const (
	taskMap CurrentTaskType = iota
	taskReduce
)

const (
	doing TaskStatus = iota
	done
	notStart
)

type Coordinator struct {
	// Your definitions here.
	*WorkerStatus
	*TaskList
	currentTaskType CurrentTaskType
	nReduce         int
	doneCh          chan int
}

type WorkerStatus struct {
	workerStatusMap  map[int]workerStatus
	workerStatusLock *sync.RWMutex
	workerIndex      int
}

type TaskList struct {
	mapTaskPath   []string
	mapTaskStatus []TaskStatus

	reduceTaskPath   [][]string
	reduceTaskStatus []TaskStatus

	taskLock   *sync.RWMutex
	mapNumbers int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.workerStatusLock.RLock()
	status, ok := c.workerStatusMap[args.MachineID]
	c.workerStatusLock.RUnlock()
	if !ok || args.MachineID == 0 {
		// 新的worker
		c.workerStatusLock.Lock()

		reply.MachineID = c.workerIndex
		c.workerStatusMap[args.MachineID] = idle
		args.MachineID = c.workerIndex

		c.workerIndex++

		c.workerStatusLock.Unlock()
	} else if status == dead {
		c.workerStatusLock.Lock()
		c.workerStatusMap[args.MachineID] = idle
		c.workerStatusLock.Unlock()
	}

	// 查看是否有可分配的任务
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	remainTask := false
	if c.currentTaskType == taskMap {
		// Map 任务
		idx := 0
		for ; idx < c.mapNumbers; idx++ {
			if c.mapTaskStatus[idx] == notStart {
				c.mapTaskStatus[idx] = doing
				remainTask = true
				break
			}
		}
		if remainTask {
			// 还有任务可以分配
			reply.TaskNumber = idx
			reply.TaskType = MapTask
			reply.Filename = []string{c.mapTaskPath[idx]}
			reply.NReduce = c.nReduce
			reply.MachineID = args.MachineID

			c.workerStatusLock.Lock()
			c.workerStatusMap[args.MachineID] = working
			c.workerStatusLock.Unlock()
		} else {
			// 没有任务分配了
			reply.TaskType = NoTask
			reply.MachineID = args.MachineID
		}
	} else {
		// Reduce 任务
		idx := 0
		for ; idx < c.nReduce; idx++ {
			if c.reduceTaskStatus[idx] == notStart {
				c.reduceTaskStatus[idx] = doing
				remainTask = true
				break
			}
		}
		if remainTask {
			// 还有任务可以分配
			reply.TaskNumber = idx
			reply.TaskType = ReduceTask
			reply.Filename = c.reduceTaskPath[idx]
			reply.NReduce = c.nReduce
			reply.MachineID = args.MachineID

			c.workerStatusLock.Lock()
			c.workerStatusMap[args.MachineID] = working
			c.workerStatusLock.Unlock()
		} else {
			// 没有任务分配了
			reply.TaskType = NoTask
			reply.MachineID = args.MachineID
		}
	}

	// 增加任务的超时定时器
	if remainTask {
		go func(taskNumber int, machineID int, taskType CurrentTaskType) {
			time.Sleep(taskExpirationTime)
			c.taskLock.Lock()
			defer c.taskLock.Unlock()
			c.workerStatusLock.Lock()
			defer c.workerStatusLock.Unlock()

			if taskType == taskMap {
				if c.mapTaskStatus[taskNumber] != done {
					c.mapTaskStatus[taskNumber] = notStart
					c.workerStatusMap[machineID] = dead
				}
			} else {
				if c.reduceTaskStatus[taskNumber] != done {
					c.reduceTaskStatus[taskNumber] = notStart
					c.workerStatusMap[machineID] = dead
				}
			}
		}(reply.TaskNumber, args.MachineID, c.currentTaskType)
	}
	return nil
}

func (c *Coordinator) TaskFail(args *TaskFailArgs, reply *TaskFailReply) error {
	c.taskLock.Lock()
	if c.currentTaskType == taskMap {
		c.mapTaskStatus[args.TaskNumber] = notStart
	} else {
		c.reduceTaskStatus[args.TaskNumber] = notStart
	}
	c.taskLock.Unlock()

	c.workerStatusLock.Lock()
	c.workerStatusMap[args.MachineID] = idle
	c.workerStatusLock.Unlock()

	return nil
}

func (c *Coordinator) Recovery(args *RecoveryArgs, reply *RecoveryReply) error {
	c.workerStatusLock.Lock()
	c.workerStatusMap[args.MachineID] = idle
	c.workerStatusLock.Unlock()

	return nil
}

func (c *Coordinator) TaskSuccess(args *TaskSuccessArgs, reply *TaskSuccessReply) error {
	if args.TaskType == MapTask {
		c.workerStatusLock.Lock()
		c.workerStatusMap[args.MachineID] = idle
		c.workerStatusLock.Unlock()

		c.taskLock.Lock()
		defer c.taskLock.Unlock()
		c.mapTaskStatus[args.TaskNumber] = done

		// 判断map任务是否全部完成
		isAllDone := true
		for _, value := range c.mapTaskStatus {
			if value != done {
				isAllDone = false
				break
			}
		}
		if isAllDone {
			c.currentTaskType = taskReduce
			// 做好reduceTask的前置工作
			for idx := 0; idx < len(c.reduceTaskStatus); idx++ {
				c.reduceTaskStatus[idx] = notStart
			}
			for idx := 0; idx < len(c.reduceTaskPath); idx++ {
				c.reduceTaskPath[idx] = make([]string, 0, c.nReduce)
			}
			filepath.Walk("./intermediate", func(path string, info fs.FileInfo, err error) error {
				if strings.HasPrefix(info.Name(), "mr-") {
					splitSlice := strings.Split(info.Name(), "-")
					reduceTaskNumber, err := strconv.Atoi(splitSlice[len(splitSlice)-1])
					if err != nil {
						log.Fatalf("collect intermediate files fail %v", err)
						return err
					}
					c.reduceTaskPath[reduceTaskNumber] = append(c.reduceTaskPath[reduceTaskNumber], info.Name())
				}
				return nil
			})
		}
	} else {
		// reduce task
		c.workerStatusLock.Lock()
		c.workerStatusMap[args.MachineID] = idle
		c.workerStatusLock.Unlock()

		c.taskLock.Lock()
		defer c.taskLock.Unlock()
		c.reduceTaskStatus[args.TaskNumber] = done

		log.Printf("reduce task %d done", args.TaskNumber)
		// 判断map任务是否全部完成
		isAllDone := true
		for _, value := range c.reduceTaskStatus {
			if value != done {
				isAllDone = false
				break
			}
		}
		if isAllDone {
			// todo 将所有output文件整合成一个，并排序
			c.doneCh <- 1
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	<-c.doneCh
	ret = true

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	var mapTaskStatus = make([]TaskStatus, len(files), len(files))
	for i := 0; i < len(mapTaskStatus); i++ {
		mapTaskStatus[i] = notStart
	}
	var reduceTaskStatus = make([]TaskStatus, nReduce, nReduce)
	var reduceTaskPath = make([][]string, nReduce, nReduce)

	c := Coordinator{
		WorkerStatus: &WorkerStatus{
			workerStatusMap:  map[int]workerStatus{},
			workerStatusLock: &sync.RWMutex{},
			workerIndex:      1,
		},
		TaskList: &TaskList{
			mapTaskPath:      files,
			mapTaskStatus:    mapTaskStatus,
			reduceTaskPath:   reduceTaskPath,
			reduceTaskStatus: reduceTaskStatus,
			taskLock:         &sync.RWMutex{},
			mapNumbers:       len(files),
		},
		currentTaskType: taskMap,
		nReduce:         nReduce,
		doneCh:          make(chan int),
	}
	c.nReduce = nReduce

	c.server()
	return &c
}
