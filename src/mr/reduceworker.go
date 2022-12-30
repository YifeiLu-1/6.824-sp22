package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
)

func doReduce(reducef func(string, []string) string, reduceTaskNum int, totalMapTaskNum int) {
	intermediate := readAllReduceFiles(reduceTaskNum, totalMapTaskNum)

	sort.Sort(ByKey(intermediate))

	tmpFile, err := ioutil.TempFile(os.TempDir(), "mr-out-")
	if err != nil {
		log.Fatal("Cannot create temporary file", err)
	}

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
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	newPath := fmt.Sprintf("../mr-tmp/mr-out-%d", reduceTaskNum)

	err = os.Rename(tmpFile.Name(), newPath)
	if err != nil {
		log.Fatalf("Rename File %v to %v failed: %v\n", tmpFile.Name(), newPath, err)
	}

	err = tmpFile.Close()
	if err != nil {
		log.Fatalf("Close File %v failed\n", tmpFile.Name())
	}
}

func readAllReduceFiles(reduceTaskNum int, totalMapTaskNum int) []KeyValue {
	filenames := []string{}
	kva := []KeyValue{}
	for mapTaskNum := 0; mapTaskNum < totalMapTaskNum; mapTaskNum++ {
		filename := fmt.Sprintf("../mr-tmp/mr-%d-%d", mapTaskNum, reduceTaskNum)
		filenames = append(filenames, filename)
		kva = append(kva, readReduceFile(filename)...)
	}

	//fmt.Printf("reduce task on %v\n", filenames)

	return kva
}

func readReduceFile(filename string) []KeyValue {
	kva := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Can't open file %v\n", filename)
	}

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	file.Close()
	return kva
}
