package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// ihash返回的是key的hash值
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// 传入mapf，reducef
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// workers周期性地向master请求任务，每次请求之间休眠 time.Sleep()
	loop := true

	for loop {
		re := RequestTask()
		switch re.Answer {
		case TaskGetted: // 当worker成功被分配到任务
			task := re.Task
			switch task.TaskType {
			case MapTask: // map任务
				fmt.Printf("A worker get a map task and taskId is %d\n", task.TaskId)
				PerformMapTask(mapf, &task)
				FinishTaskAndReport(task.TaskId)
			case ReduceTask: // reduce任务
				fmt.Printf("A worker get a reduce task and taskId is %d\n", task.TaskId)
				PerformReduceTask(reducef, &task)
				FinishTaskAndReport(task.TaskId)
			}
		case WaitPlz: // 本次请求并未分得任务，worker等待1s后再下次请求
			time.Sleep(time.Second)
		case FinishAndExit: // mapreduce已经处于AllDone阶段，worker准备退出
			loop = false
		default:
			fmt.Println("request task error!")
		}
	}

}

//// example function to show how to make an RPC call to the master.
////
//// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Master.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}

// 向master请求RPC,获取任务
func RequestTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	// 请求调用Master的AssignTask方法
	ok := call("Master.AssignTask", &args, &reply)

	if !ok { // 请求失败
		fmt.Println("Call failed!")
	}

	return reply
}

// worker完成任务后调用RPC告知master并请求修改任务状态
func FinishTaskAndReport(id int) {
	args := FinArgs{TaskId: id}
	reply := FinReply{}

	// 请求调用Master的UpdateTaskState方法
	ok := call("Master.UpdateTaskState", &args, &reply)
	if !ok { //请求失败
		fmt.Println("Call failed!")
	}
}

// 执行map任务
// mapf和reducef是mrworker.go创建worker时传进来的
// task是向Master请求后返回的待处理的任务
func PerformMapTask(mapf func(string, string) []KeyValue, task *Task) {
	intermediate := []KeyValue{}              // 存储map处理后的中间kv对
	for _, filename := range task.InputFile { // map阶段每个worker事实上输入只有一个文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		// ReadAll: 返回读取的数据和遇到的错误。成功的调用返回的err为nil而非EOF。
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content)) // 调用传入的map函数
		intermediate = append(intermediate, kva...)
	}

	// 对中间键值对按key排序（在这里排序似乎没有必要，因为即使在这个map任务内排序，后面将对应地多个中间文件输入reduce也需要整体再排序）
	//sort.Sort(ByKey(intermediate))

	// 将中间键值对写入中间tmp文件
	// 中间文件的合理命名方式是 `mr-X-Y` ，X是Map任务的序号，Y是ihash(key)%nreduce后的值，这样就建立了key与被分配给的reduce任务的映射关系
	// map任务。可以使用Go的`encoding/json` package去存储中间的键值对到文件以便reduce任务之后读取
	rn := task.ReducerNum
	for i := 0; i < rn; i++ {
		// 创建中间输出文件
		midFileName := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		midFile, _ := os.Create(midFileName)
		enc := json.NewEncoder(midFile)   // NewEncoder创建一个将数据写入w的*Encoder
		for _, kv := range intermediate { // 遍历intermediate寻找写入该中间文件的kv
			if ihash(kv.Key)%rn == i {
				enc.Encode(&kv) // Encode将v的json编码写入输出流，并会写入一个换行符
			}
		}
		midFile.Close()
	}
}

// 执行reduce任务
func PerformReduceTask(reducef func(string, []string) string, task *Task) {
	intermediate := []KeyValue{} // 存储该reduce任务所有输入的中间文件中的kv（后需对其排序）

	intermediate = shuffle(task.InputFile) // 对reduce的所有输入中间文件洗牌得到一个有序的kv切片

	// 为了确保没有人在出现崩溃的情况下观察到部分写入的文件，MapReduce论文提到了使用临时文件并在完成写入后原子地重命名它的技巧
	// 使用 ioutil.TempFile 来创建一个临时文件以及 os.Rename 来原子性地重命名它

	dir, _ := os.Getwd()
	// CreateTemp 在目录dir中新建一个临时文件，打开文件进行读写，返回结果文件
	// pattern 这是用于生成临时文件的名称。该名称是通过在模式的末尾附加一个随机字符串来创建的
	tmpfile, err := os.CreateTemp(dir, "mr-out-tmpfile-")
	if err != nil {
		log.Fatal("failed to create temp file", err)
	}

	// 对中间键值对中每个不同的key调用一次reduce，注意此时intermediate已经按key排好序了
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 如果i,j指向的两个key相同则继续向后寻找不同的key
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}     // len为0的切片
		for k := i; k < j; k++ { // 最后values形如{1，1，1，...}
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values) // 调用传入的reduce函数，输出的结果是该单词在这些文本中出现的次数

		// this is the correct format for each line of Reduce output.
		// 将reduce的输出写入临时文件
		// Fprintf根据format参数生成格式化的字符串并写入w。返回写入的字节数和遇到的任何错误
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpfile.Close()

	// reudce的输出文件，命名格式为：mr-out-*，其中*通过task记录的ReducerKth获取
	oname := "mr-out-" + strconv.Itoa(task.ReducerKth)

	os.Rename(dir+tmpfile.Name(), dir+oname)
}

// shuffle函数，将一个reduce任务的所有输入中间文件中的kv排序
func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}              // 存储该reduce任务所有输入的中间文件中的kv
	for _, filename := range files { // reduce阶段每个worker的输入为若干个中间文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		// 反序列化JSON格式文件
		dec := json.NewDecoder(file)
		// 读取文件内容
		for {
			var kv KeyValue
			// func (dec *Decoder) Decode(v interface{}) error
			// Decode从输入流读取下一个json编码值并保存在v指向的值里
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv) // 将中间文件的每组kv都写入kva
		}
		file.Close()
	}
	// 此时kva存放了该reduce任务的所有输入中间文件中的kv
	// 将中间键值对按key排序
	sort.Sort(ByKey(kva)) // 排序后kva形如：[{a,1},{a,1},{b,1},{c,1}...]
	return kva
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// 这个函数相当于把客户端远程调用rpcname的过程封装了一下，不用每次都写下面的代码呼叫server，执行远程调用等
// rpcname是要调用的服务端的方法如"Master.Example"
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	// DialHTTPPath在指定的网络、地址和路径与HTTP RPC服务端连接。
	// c是client
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// 客户端执行远程调用
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true // 正常情况返回true
	}

	fmt.Println(err)
	return false // 失败返回false
}
