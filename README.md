# A Golang implementation of distributed key-value server 

This is the source code of MIT 6.824 spring 2022 projects, which features 4 labs

lab1: implement Google's MapReduce library

lab2: implement RAFT distributed consensus module, including leader election, log, persistence and snapshot log compaction

lab3: build a key-value server on top of lab2, providing linearizability

lab4: shard the key-value to multiple raft groups

lab2 passes the test 2000 times

lab3 passes the test 1000 times

lab4a is finished, lab4b is still in progress

## Run the tests

```sh
$ go test ./raft
$ go test ./kvraft
$ go test ./shardctrler
$ go test ./shardkv
```
