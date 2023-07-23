package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)
import "hash/fnv"

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
	fmt.Println("worker has start")
	for {
		task := &Task{}
		err := getTask(task)
		if err != nil {
			fmt.Println(err)
			return
		}
		switch task.TaskStatus {
		case Preparation:
			//fmt.Println("error ", task.TaskId, task.TaskStatus)
		case Map:
			filepath := task.FilePath[0]
			context, err := ioutil.ReadFile(filepath)
			if err != nil {
				//fmt.Println("Worker_Map open file", err)
				return
			}
			intermediate := mapf(filepath, string(context))
			//写入文件
			rn := task.ReducerNum
			// 创建一个长度为nReduce的二维切片
			HashedKV := make([][]KeyValue, rn)
			for _, kv := range intermediate {
				HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
			}
			task.FilePath = []string{}
			for i := 0; i < rn; i++ {
				oName := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
				task.FilePath = append(task.FilePath, oName)
				oFile, _ := os.Create(oName)
				enc := json.NewEncoder(oFile)
				for _, kv := range HashedKV[i] {
					enc.Encode(kv)
				}
				oFile.Close()
			}
			//返回给协调者
			err = dealTask(task)
			if err != nil {
				//fmt.Println("Worker_Map returnTask", err)
				return
			}
		case Reduce:
			var intermediate []KeyValue
			for _, filepath := range task.FilePath {
				file, _ := os.Open(filepath)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})
			oName := fmt.Sprintf("mr-out-%d", task.TaskId)
			oFile, _ := os.Create(oName)
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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			oFile.Close()
			//发送给协调者
			err = dealTask(task)
			if err != nil {
				//fmt.Println("Worker_Reduce returnTask", err)
				return
			}
		case Done:
			return
		default:
			fmt.Println("all tasks has done")
		}
	}

}

func getTask(task *Task) error {
	args := ExampleArgs{}
	ok := call("Coordinator.GetTask", &args, task)
	if !ok {
		return errors.New("call fail GetTask")
	}
	return nil
}

func dealTask(task *Task) (err error) {
	reply := ExampleReply{}
	//fmt.Println(task)
	ok := call("Coordinator.DealTask", task, &reply)
	if !ok {
		return errors.New("call fail DealTask")
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
