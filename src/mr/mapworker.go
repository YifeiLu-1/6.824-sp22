package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"sort"
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(mapf func(string, string) []KeyValue, filename string, nReduce int, mapTaskNum int) {
	intermediate := []KeyValue{}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	nBuckets := hashIntoNBuckets(intermediate, nReduce)
	sortEachBucket(nBuckets)
	outputBucketsToNFiles(nBuckets, mapTaskNum)
	//fmt.Printf("finished map %v task, produced\n", filename)
}

func hashIntoNBuckets(intermediates []KeyValue, nReduce int) [][]KeyValue {
	nBuckets := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		nBuckets[i] = []KeyValue{}
	}
	var bucketNum int
	for _, pair := range intermediates {
		bucketNum = ihash(pair.Key) % nReduce
		nBuckets[bucketNum] = append(nBuckets[bucketNum], pair)
	}
	return nBuckets
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func sortEachBucket(nBuckets [][]KeyValue) {
	for _, bucket := range nBuckets {
		sort.Sort(ByKey(bucket))
	}
}

func outputBucketsToNFiles(nBuckets [][]KeyValue, taskNum int) {
	for index, bucket := range nBuckets {
		outputBucketToFile(bucket, taskNum, index)
	}
}

func outputBucketToFile(bucket []KeyValue, mapTaskNum int, reduceTaskNum int) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "mr-intermediate-")
	if err != nil {
		log.Fatal("Cannot create temporary file", err)
	}

	enc := json.NewEncoder(tmpFile)
	for _, kv := range bucket {
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Encode key value pair %v failed on temporary file %v", kv, tmpFile)
		}
	}

	newPath := fmt.Sprintf("../mr-tmp/mr-%d-%d", mapTaskNum, reduceTaskNum)

	err = os.Rename(tmpFile.Name(), newPath)
	if err != nil {
		log.Fatalf("Rename File %v to %v failed: %v\n", tmpFile.Name(), newPath, err)
	}

	err = tmpFile.Close()
	if err != nil {
		log.Fatalf("Close File %v failed\n", tmpFile.Name())
	}

}
