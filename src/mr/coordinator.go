package mr

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var reduceMap map[int][]string
var lockGetTask sync.Mutex
var lockDealTask sync.Mutex
var Status map[int]string

const (
	StatusPreparation = iota + 1
	StatusMap
	StatusReduce
	StatusDone
)

type Coordinator struct {
	// Your definitions here.
	Status     int
	TaskChan   chan *Task
	TaskMap    map[int]int
	CancelMap  map[int]context.CancelFunc
	ReducerNum int
}

func (c *Coordinator) ChangeCoordinatorStatus() {
	switch c.Status {
	case StatusPreparation:
		reduceMap = make(map[int][]string)
		for i := 0; i < c.ReducerNum; i++ {
			reduceMap[i] = make([]string, 0)
		}
		fmt.Printf("Coordinator status change from %s to %s\n", Status[c.Status], Status[c.Status+1])
		c.Status++
	case StatusMap:
		//生成reduce任务
		c.TaskMap = make(map[int]int)
		for i := 0; i < c.ReducerNum; i++ {
			id := GenerateTaskId()
			task := &Task{
				TaskId:     id,
				TaskStatus: Reduce,
				ReducerNum: c.ReducerNum,
				FilePath:   reduceMap[i],
			}
			c.TaskMap[task.TaskId] = Reduce
			c.TaskChan <- task
		}
		fmt.Printf("Coordinator status change from %s to %s\n", Status[c.Status], Status[c.Status+1])
		c.Status++
	case StatusReduce:
		fmt.Printf("Coordinator status change from %s to %s\n", Status[c.Status], Status[c.Status+1])
		c.Status++
	default:
	}
}

func (c *Coordinator) GetTask(args *ExampleArgs, task *Task) error {
	lockGetTask.Lock()
	defer lockGetTask.Unlock()
	switch c.Status {
	case StatusPreparation:
		fmt.Println("errorStatus getTask", c.Status)
		return nil
	case StatusMap:
		*task = *<-c.TaskChan
		//开启一个ctx,用于监听任务是否超时,完成crash——test
		ctx, cancel := context.WithCancel(context.Background())
		c.CancelMap[task.TaskId] = cancel
		go func() {
			time.Sleep(time.Second * 10)
			select {
			case <-ctx.Done():
				return
			default:
				c.TaskChan <- task
			}
		}()
		return nil
	case StatusReduce:
		*task = *<-c.TaskChan
		ctx, cancel := context.WithCancel(context.Background())
		c.CancelMap[task.TaskId] = cancel
		go func() {
			time.Sleep(time.Second * 10)
			select {
			case <-ctx.Done():
				return
			default:
				c.TaskChan <- task
			}
		}()
		return nil
	case StatusDone:
		task.TaskStatus = Done
		return nil
	default:
		return errors.New("error_status")
	}
}

func (c *Coordinator) DealTask(task *Task, reply *ExampleReply) error {
	lockDealTask.Lock()
	defer lockDealTask.Unlock()
	//存在竞态问题
	switch task.TaskStatus {
	case Preparation:
		task.TaskStatus = Map
		c.TaskMap[task.TaskId] = Map
		return nil
	case Map:
		//Map标记为完成
		c.TaskMap[task.TaskId] = Reduce
		c.CancelMap[task.TaskId]()
		//记录以reduce进行任务的划分
		for i := 0; i < len(task.FilePath); i++ {
			strs := strings.Split(task.FilePath[i], "-")
			num, _ := strconv.Atoi(strs[len(strs)-1])
			reduceMap[num] = append(reduceMap[num], task.FilePath[i])
		}
		//如果所有的map任务都被处理完毕,则执行cancel进入下一阶段
		if c.checkTaskDone() {
			c.ChangeCoordinatorStatus()
		}
		return nil
	case Reduce:
		c.TaskMap[task.TaskId] = Done
		c.CancelMap[task.TaskId]()
		if c.checkTaskDone() {
			c.ChangeCoordinatorStatus()
		}
		return nil
	default:
		fmt.Println("err DealTask", task.TaskId, task.TaskStatus)
		return errors.New("err DealTask")
	}
}

func (c *Coordinator) checkTaskDone() bool {
	switch c.Status {
	case StatusMap:
		for _, v := range c.TaskMap {
			if v != Reduce {
				return false
			}
		}
		return true
	case StatusReduce:
		for _, v := range c.TaskMap {
			if v != Done {
				return false
			}
		}
		return true
	default:
		//fmt.Println("err checkTaskDone", c.Status)
		return false
	}
}

// Your code here -- RPC handlers forf the worker to call.

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
	// Your code here.
	return c.Status == Done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//初始化reduceMap
	lockGetTask = sync.Mutex{}
	lockDealTask = sync.Mutex{}
	Status = map[int]string{
		StatusPreparation: "Preparation",
		StatusMap:         "Map",
		StatusReduce:      "Reduce",
		StatusDone:        "Done",
	}
	//初始化协调者
	c := Coordinator{
		Status:     StatusPreparation,
		TaskChan:   make(chan *Task, len(files)+nReduce),
		TaskMap:    make(map[int]int),
		CancelMap:  make(map[int]context.CancelFunc),
		ReducerNum: nReduce,
	}
	//初始化Task
	for _, f := range files {
		task := &Task{
			TaskId:     GenerateTaskId(),
			TaskStatus: Preparation,
			FilePath:   []string{f},
			ReducerNum: nReduce,
		}
		//进入管道
		c.DealTask(task, nil)
		c.TaskChan <- task
	}
	c.ChangeCoordinatorStatus()
	// Your code here.
	c.server()
	fmt.Println("Coordinator has start")
	return &c
}
