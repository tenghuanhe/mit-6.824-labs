package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	waiting := make(chan string)
	jobs := make(chan *DoJobArgs)
	done := make(chan struct{})

	workerDoJob := func(addr string, args *DoJobArgs) {
		var reply DoJobReply
		ok := call(addr, "Worker.DoJob", args, &reply)
		if ok {
			done <- struct{}{}
			waiting <- addr
		} else {
			jobs <- args
		}
	}

	getNextWorkerAddr := func() string {
		var addr string
		select {
		case addr = <-mr.registerChannel:
			mr.Workers[addr] = &WorkerInfo{addr}
		case addr = <-waiting:
		}
		return addr
	}

	// Scheduler
	go func() {
		for job := range jobs {
			addr := getNextWorkerAddr()
			go workerDoJob(addr, job)
		}
	}()

	go func() {
		for i := 0; i < mr.nMap; i++ {
			args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
			jobs <- args
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		<-done
	}

	go func() {
		for i := 0; i < mr.nReduce; i++ {
			args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
			jobs <- args
		}
	}()

	for i := 0; i < mr.nReduce; i++ {
		<-done
	}

	close(jobs)

	return mr.KillWorkers()
}
