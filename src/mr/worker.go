package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"syscall"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var (
	sleepTime          = 1 * time.Second
	lockExpirationTime = 10 * time.Second
	machineID          = 0
)

const (
	RPCFunctionGetTask     = "Coordinator.GetTask"
	RPCFunctionTaskFail    = "Coordinator.TaskFail"
	RPCFunctionRecovery    = "Coordinator.Recovery"
	RPCFunctionTaskSuccess = "Coordinator.TaskSuccess"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	/* 某个worker crash或者超时
	每个map任务对应一个文件，作为锁。map任务每次写入一个kv到磁盘前都先要查看这个文件上的machineID是否为自己
	抢到锁后要像极客时间那样设置有效期
	① master轮询抢占锁，抢占到后清除锁文件中的内容，删除或清空原有worker写入的内容，然后释放锁
	② 将这个worker状态记为crash
	③ 将map任务分配给一个新的worker，新的worker自己抢占锁，并加上超时时间
	④ 旧的worker如果不是crash，当它发现文件上的machineID不是自己时，给master发送一个rpc消息，master接收到后恢复这个worker的状态为空闲

	*/

	for true {
		args := GetTaskArgs{
			MachineID: machineID,
		}
		reply := GetTaskReply{}
		call(RPCFunctionGetTask, &args, &reply)

		machineID = reply.MachineID

		if reply.TaskType != NoTask {

			// 加锁
			lockFilePath := fmt.Sprintf("./lockfile_%d", reply.TaskNumber)
			lockFile, err := os.OpenFile(lockFilePath, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				// todo send rpc fail message
				sendFailTaskRPC(reply.TaskNumber)
				return
			}
			err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if err != nil {
				// todo send rpc fail message
				sendFailTaskRPC(reply.TaskNumber)
				return
			}
			go func() {
				time.Sleep(lockExpirationTime)
				syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
				lockFile.Close()
			}()
			_, err = lockFile.WriteString(fmt.Sprintf("%d", reply.MachineID))
			if err != nil {
				// todo send rpc fail message
				sendFailTaskRPC(reply.TaskNumber)
				return
			}

			if reply.TaskType == MapTask {
				// 读取文件并进行map
				filename := reply.Filename[0]
				intermediate := []KeyValue{}
				file, err := os.Open("./main/" + filename)
				if err != nil {
					log.Fatalf("cannot open %v %s", filename, err.Error())
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)

				// 将中间结果输出到磁盘
				intermediateFileMap := map[string]string{}
				for _, ele := range intermediate {
					fileBytes, err2 := ioutil.ReadFile(lockFilePath)
					if err2 != nil {
						// todo send rpc fail message
						sendFailTaskRPC(reply.TaskNumber)
						return
					}
					machineIDFromLockFile, err2 := strconv.Atoi(string(fileBytes))
					if err2 != nil {
						// todo send rpc fail message
						sendFailTaskRPC(reply.TaskNumber)
						return
					}
					if machineIDFromLockFile != machineID {
						// todo send rpc recovery message
						sendRecoveryRPC(reply.TaskNumber)
						return
					}
					reduceTaskNumber := ihash(ele.Key) % reply.NReduce
					intermediateFilename := fmt.Sprintf("mr-%d-%d", reply.TaskNumber, reduceTaskNumber)
					// 如果有上一个worker未完成的数据，需要先删除
					if _, ok := intermediateFileMap[intermediateFilename]; !ok {
						_, err2 = os.Stat("./intermediate/" + intermediateFilename)
						if os.IsExist(err2) {
							err2 = os.Remove("./intermediate/" + intermediateFilename)
							if err2 != nil {
								// todo send rpc fail message
								sendFailTaskRPC(reply.TaskNumber)
								return
							}
						}
						intermediateFileMap[intermediateFilename] = "exist"
					}
					_ = os.Mkdir("intermediate", os.ModePerm)
					intermediateFile, err2 := os.OpenFile("./intermediate/"+intermediateFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
					if err2 != nil {
						// todo send rpc fail message
						sendFailTaskRPC(reply.TaskNumber)
						return
					}
					// fmt.Fprintf(intermediateFile, "%v %v\n", ele.Key, ele.Value)
					encoder := json.NewEncoder(intermediateFile)
					err2 = encoder.Encode(&ele)
					if err2 != nil {
						// todo send rpc fail message
						sendFailTaskRPC(reply.TaskNumber)
						return
					}
				}
				// 完成 map任务
				successArgs := TaskSuccessArgs{
					MachineID:  machineID,
					TaskNumber: reply.TaskNumber,
					TaskType:   MapTask,
				}
				successReply := TaskSuccessReply{}
				call(RPCFunctionTaskSuccess, &successArgs, &successReply)
			} else {
				// reduce 任务
				// 读入输出位置
				outputFilename := fmt.Sprintf("mr-out-%d", reply.TaskNumber)
				outputFile, err2 := os.OpenFile(outputFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
				if err2 != nil {
					// todo send rpc fail message
					sendFailTaskRPC(reply.TaskNumber)
					return
				}
				// 如果有上一个worker未完成的数据，需要先删除
				_, err2 = os.Stat(outputFilename)
				if os.IsExist(err2) {
					err2 = os.Remove(outputFilename)
					if err2 != nil {
						// todo send rpc fail message
						sendFailTaskRPC(reply.TaskNumber)
						return
					}
				}
				// 读取intermediate文件
				filenames := reply.Filename
				intermediate := []KeyValue{}
				for _, filename := range filenames {
					// 读入中间结果
					file, err2 := os.OpenFile("./intermediate/"+filename, os.O_RDONLY, 0644)
					if err2 != nil {
						sendFailTaskRPC(reply.TaskNumber)
						return
					}
					decoder := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := decoder.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
				}
				// 排序intermediate
				log.Printf("reduce task %d", reply.TaskNumber)
				sort.Sort(ByKey(intermediate))
				//读完所有的intermediate，开始reduce
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)
					fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
				successArgs := TaskSuccessArgs{
					MachineID:  machineID,
					TaskNumber: reply.TaskNumber,
					TaskType:   ReduceTask,
				}
				successReply := TaskSuccessReply{}
				call(RPCFunctionTaskSuccess, &successArgs, &successReply)
			}
		}

		time.Sleep(sleepTime)
	}

}

func sendFailTaskRPC(taskNumber int) {
	args := TaskFailArgs{
		TaskNumber: taskNumber,
		MachineID:  machineID,
	}
	reply := TaskFailReply{}
	call(RPCFunctionTaskFail, &args, &reply)
}

func sendRecoveryRPC(machineID int) {
	args := RecoveryArgs{
		MachineID: machineID,
	}
	reply := RecoveryReply{}
	call(RPCFunctionRecovery, &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
