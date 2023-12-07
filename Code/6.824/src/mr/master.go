package mr

import (
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

// 定义全局互斥锁，worker访问Master时加锁。用于保证函数内并发安全
var mu sync.Mutex

// 任务类型
type TaskType int

// 任务状态
type TaskState int

// MapReduce执行到的阶段
type Phase int

// 枚举任务类型
const (
	MapTask TaskType = iota
	ReduceTask
)

// 枚举任务状态
const (
	Working TaskState = iota // 任务正在进行
	Waiting                  // 任务正在等待执行
	Finshed                  // 任务已经完成
)

// 枚举MapReduce执行阶段
const (
	MapPhase    Phase = iota // Map阶段
	ReducePhase              // Reduce阶段
	AllDone                  // 全部完成
)

// 定义任务的结构体
type Task struct {
	TaskId     int       // 任务id（唯一），通过Master生成，从0开始
	TaskType   TaskType  // 任务类型
	TaskState  TaskState // 任务状态
	InputFile  []string  // 输入文件名切片，map情况切片里面就一个分配给worker的文件,reduce情况下是worker需要选取的一些需要聚合到一起的中间文件
	ReducerNum int       // 传入的reducer的数量，用于hash
	ReducerKth int       // 表明reducer的序号（最终输出文件命名时需要），不等于taskId，只有为reduce任务时才有意义
	StartTime  time.Time // 任务被分配给worker后开始执行的时间（用于crash判断）
}

// channel 是golang特有的类型化消息的队列，可以通过它们发送类型化的数据在协程之间通信，可以避开所有内存共享导致的坑；通道的通信方式保证了同步性。
type Master struct {
	// Your definitions here.
	CurrentPhase      Phase         // 当前MapReduce处于什么阶段
	TaskIdForGen      int           // 用于生成任务唯一TaskId
	MapTaskChannel    chan *Task    // 管理待执行的Map任务，使用chan保证并发安全
	ReduceTaskChannel chan *Task    // 管理待执行的Reduce任务，使用chan保证并发安全
	TaskMap           map[int]*Task // 记录所有Task信息，方便Master管理任务，获得任务进行情况（当任务分配后更新）
	MapperNum         int           // mapper的数量，由于每个文件分给一个mapper做，因此就等于输入文件的个数
	ReducerNum        int           // 传入的reducer的数量，用于hash
}

//// Your code here -- RPC handlers for the worker to call.
//
//// an example RPC handler.
////
//// the RPC argument and reply types are defined in rpc.go.
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// Master给Worker分配任务的函数
func (m *Master) AssignTask(args *TaskArgs, reply *TaskReply) error {
	// 分配任务需要上锁，防止多个worker竞争，保证并发安全，并用defer回退解锁
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("Master gets a request from worker:")

	// Master根据当前执行阶段来判断执行什么操作
	switch m.CurrentPhase {
	case MapPhase:
		// 先检查MapTaskChannel里是否还有未分配的任务
		if len(m.MapTaskChannel) > 0 { // 还有未分配的map任务
			taskp := <-m.MapTaskChannel     // 取出的是指针
			if taskp.TaskState == Waiting { // 若此map任务正等待执行
				//*reply = TaskReply{*taskp}
				reply.Answer = TaskGetted // 请求到了map任务
				reply.Task = *taskp
				taskp.TaskState = Working          // 将任务状态改为Working，代表已被分配给了worker
				taskp.StartTime = time.Now()       // 更新任务开始执行的时间
				m.TaskMap[(*taskp).TaskId] = taskp // 分配任务的同时将其加入TaskMap
				fmt.Printf("Task[%d] has been assigned.\n", taskp.TaskId)
			}
		} else { // 没有未分配的map任务，检查map是否都执行完毕了，若是则Master转向reduce阶段
			reply.Answer = WaitPlz // 本次请求暂无任务分配
			// 检查map是否都执行完毕了
			if m.checkMapTaskDone() { // map阶段任务都执行完了
				m.toNextPhase() // 转向下一阶段
			}
			return nil
		}
	case ReducePhase:
		// 先检查ReduceTaskChannel里是否还有未分配的任务
		if len(m.ReduceTaskChannel) > 0 { // 还有未分配的reduce任务
			taskp := <-m.ReduceTaskChannel  // 取出的是指针
			if taskp.TaskState == Waiting { // 若此reduce任务正等待执行
				//*reply = TaskReply{*taskp}
				reply.Answer = TaskGetted // 请求到了reduce任务
				reply.Task = *taskp
				taskp.TaskState = Working          // 将任务状态改为Working，代表已被分配给了worker
				taskp.StartTime = time.Now()       // 更新任务开始执行的时间
				m.TaskMap[(*taskp).TaskId] = taskp // 分配任务的同时将其加入TaskMap
				fmt.Printf("Task[%d] has been assigned.\n", taskp.TaskId)
			}
		} else { // 没有未分配的reduce任务，检查map是否都执行完毕了，若是则Master转向AllDone阶段
			reply.Answer = WaitPlz // 本次请求暂无任务分配
			// 检查map是否都执行完毕了
			if m.checkReduceTaskDone() { // reduce阶段任务都执行完了
				m.toNextPhase() // 转向下一阶段
			}
			return nil
		}
	case AllDone:
		// map和reduce任务都执行完毕，本次请求的回复通知worker准备退出
		reply.Answer = FinishAndExit
		fmt.Println("All tasks have been finished!")
	default:
		panic("The phase undefined!!!")
	}

	return nil // 整个函数的返回error
}

// 当worker完成任务时，会调用RPC通知master
// master得知后负责修改worker完成任务的状态,以便master检查该阶段任务是否全部已完成
func (m *Master) UpdateTaskState(args *FinArgs, reply *FinReply) error {
	mu.Lock()
	defer mu.Unlock()
	id := args.TaskId
	fmt.Printf("Task[%d] has been finished!\n", id)

	// 由于创建任务时map任务和reduce任务被分配的taskId是唯一的，因此可以通过taskId在TaskMap中找到对应的Task
	m.TaskMap[id].TaskState = Finshed // 将TaskMap中对应的Task状态修改为Finished
	return nil
}

// Master检查map阶段任务是否全部完成，若是则转入reduce阶段
func (m *Master) checkMapTaskDone() bool {
	var (
		mapDoneNum   = 0
		mapUnDoneNum = 0
	)

	// 遍历TaskMap
	for _, v := range m.TaskMap {
		if v.TaskType == MapTask { // map任务
			if v.TaskState == Finshed {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		}
	}

	if mapDoneNum == m.MapperNum && mapUnDoneNum == 0 { // 当map阶段任务全做完才开始reduce阶段
		return true
	}

	return false
}

// Master检查reduce阶段任务是否全部完成，若是则工作完成转入AllDone阶段，准备结束程序
func (m *Master) checkReduceTaskDone() bool {
	var (
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	// 遍历TaskMap
	for _, v := range m.TaskMap {
		if v.TaskType == ReduceTask { // reduce任务
			if v.TaskState == Finshed {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}

	if reduceDoneNum == m.ReducerNum && reduceUnDoneNum == 0 {
		return true
	}

	return false
}

// Master在判断当前阶段工作已完成后转入下一阶段
func (m *Master) toNextPhase() {
	switch m.CurrentPhase {
	case MapPhase:
		m.CurrentPhase = ReducePhase // 若当前为Map阶段则下一阶段为Reduce阶段
		m.MakeReduceTask()           // Master开始生成reduce任务
	case ReducePhase:
		m.CurrentPhase = AllDone // 若当前为Reduce阶段则下一阶段为AllDone阶段
	}
}

// rpc包提供了通过网络或其他I/O连接对一个对象的导出方法的访问。
// 服务端注册一个对象，使它作为一个服务被暴露，服务的名字是该对象的类型名。
// 注册之后，对象的导出方法就可以被远程访问。服务端可以注册多个不同类型的对象（服务），但注册具有相同类型的多个对象是错误的。
// 方法必须看起来像这样：func (t *T) MethodName(argType T1, replyType *T2) error
// start a thread that listens for RPCs from worker.go
// 此时，客户端可看到服务"Master"及它的方法"Example"。要调用方法，客户端首先呼叫服务端
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

// main/mrmaster.go 周期性地调用 Done() 来检查整个工作是否已完成
// 通过检查当前mapreduce是否处于AllDone阶段
func (m *Master) Done() bool {
	ret := false
	mu.Lock() // 先加锁，因为master在执行其他过程中也可能正在访问CurrentPhase
	defer mu.Unlock()
	if m.CurrentPhase == AllDone {
		ret = true
	}
	return ret
}

// 通过自增产生TaskId
func (m *Master) GenerateTaskId() int {
	res := m.TaskIdForGen
	m.TaskIdForGen++
	return res
}

// Master生成Map任务
func (m *Master) MakeMapTask(files []string) {
	fmt.Println("begin make map tasks...")

	// 遍历输入的文本文件，向map管道中加入map任务
	for _, file := range files {
		id := m.GenerateTaskId()
		input := []string{file}
		mapTask := Task{
			TaskId:     id,
			TaskType:   MapTask,
			TaskState:  Waiting, // 尚未分配给worker时任务状态为waiting
			InputFile:  input,   // map情况切片里面就一个分配给worker的文件
			ReducerNum: m.ReducerNum,
		}
		fmt.Printf("make a map task %d\n", id)
		m.MapTaskChannel <- &mapTask
	}
}

// Master生成Reduce任务
func (m *Master) MakeReduceTask() {
	fmt.Println("begin make reduce tasks...")
	rn := m.ReducerNum
	// Getwd返回一个对应当前工作目录的根路径
	dir, _ := os.Getwd()
	files, err := os.ReadDir(dir) // files为当前目录下所有的文件
	if err != nil {
		fmt.Println(err)
	}
	for i := 0; i < rn; i++ { // 总共生成rn个reduce任务
		id := m.GenerateTaskId()
		input := []string{} // reduce任务的输入是对应的中间文件
		// 生成的中间文件mr-X-Y存放在当前文件夹
		// 从files中找到对应的输入此reduce任务的中间文件（利用前缀后缀）
		for _, file := range files { // 注意file是文件类型，判断前后缀需要用file.Name()转换成对应的文件名字符串
			if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
				input = append(input, file.Name())
			}
		}

		reduceTask := Task{
			TaskId:     id,
			TaskType:   ReduceTask,
			TaskState:  Waiting,
			InputFile:  input,
			ReducerNum: m.ReducerNum,
			ReducerKth: i,
		}
		fmt.Printf("make a reduce task %d\n", id)
		m.ReduceTaskChannel <- &reduceTask
	}
}

// master不能可靠地区分崩溃的工作线程、还活着但由于某种原因而停滞的工作线程和正在执行但速度太慢而无法使用的工作线程。
// 可以让master等待一段时间（如10s），然后放弃并将任务重新分配给其他worker，在此之后，master应该认为那个worker已经死亡
func (m *Master) CrashHandle() {
	for {
		time.Sleep(time.Second)        // 每1秒做一次判断
		mu.Lock()                      // 访问master的共享资源先加锁
		if m.CurrentPhase == AllDone { // 所有任务都完成了就不用再判断crash了
			mu.Unlock()
			break
		}

		for _, task := range m.TaskMap {
			// Since()函数保留时间值，并用于评估与实际时间的差异
			// time.Since(t)等价于time.Now().Sub(t)
			// 当任务处于Working状态持续10s以上时认为crash
			if task.TaskState == Working && time.Since(task.StartTime) > 10*time.Second {
				fmt.Printf("Task[%d] is crashed!\n", task.TaskId)
				// 将该任务重置当作未分配的任务重新加入MapTaskChannel等待分配
				// StartTime会在任务重新被分配给worker时更新
				task.TaskState = Waiting // 更新状态为待分配
				switch task.TaskType {   // 加入对应的channel等待分配给其他worker
				case MapTask:
					m.MapTaskChannel <- task
				case ReduceTask:
					m.ReduceTaskChannel <- task
				}
				delete(m.TaskMap, task.TaskId) // 将crash的任务从TaskMap中删除，等到再分配时添加回来
				// 这是因为保证TaskMap中只有分配给worker的Task记录（正在执行和已完成），未分配的Task放在channel里
			}
		}
		mu.Unlock()
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	fmt.Println("begin make a master...")
	m := Master{
		CurrentPhase:      MapPhase,                     // 开始为Map阶段
		TaskIdForGen:      0,                            // 初始为0，后面通过自增得到唯一的TaskId分配任务
		MapTaskChannel:    make(chan *Task, len(files)), // 输入文件个数决定map任务个数
		ReduceTaskChannel: make(chan *Task, nReduce),
		TaskMap:           make(map[int]*Task, len(files)+nReduce), // 最多为map任务和reduce任务的和
		MapperNum:         len(files),
		ReducerNum:        nReduce,
	}

	m.MakeMapTask(files) // 根据files生成Map任务

	m.server()

	go m.CrashHandle() // 开启探测并处理crash的协程
	return &m
}
