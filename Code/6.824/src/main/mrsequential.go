package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
	"io"
)
import "6.824/src/mr"
import "plugin"
import "os"
import "log"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
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
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	// fmt.Printf("intermediate.type = %T, ByKey(intermediate).type = %T\n", intermediate, ByKey(intermediate))
	// ByKey(intermediate)作了个类型转换，其实底层类型一样
	sort.Sort(ByKey(intermediate))

	// 创建输出文件
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		// 将reduce的输出写入文件
		// Fprintf根据format参数生成格式化的字符串并写入w。返回写入的字节数和遇到的任何错误
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
// return map和reduce两个函数
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	// plugin.Open打开一个Go的插件，返回*Plugin
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	// 在插件中寻找“Map”，找的可以是任何导出的变量或函数，返回的是一个空接口，指向函数或变量
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
