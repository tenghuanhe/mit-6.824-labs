package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"
import "io/ioutil"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var (
	Trace *log.Logger
	Info  *log.Logger
)

const (
	Put     = "Put"
	PutHash = "PutHash"
	Get     = "Get"
	Nop     = "Nop"
)

const (
	Active    = "Active"
	Committed = "Committed"
	Executed  = "Executed"
)

type Op struct {
	Id    int64
	Type  string
	Key   string
	Value string
}

type ExecuteRequest struct {
	seq          int
	responseChan chan interface{}
}

type RequestState struct {
	xid     int64
	state   string
	seq     int
	waiting []chan interface{}
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos
	requests   map[int64]*RequestState
	subscribe  chan ExecuteRequest
}

func init() {
	log.SetOutput(ioutil.Discard)
	// Trace = log.New(os.Stdout, "", log.Lmicroseconds)
	Trace = log.New(ioutil.Discard, "", log.Lmicroseconds)
	// Info = log.New(os.Stdout, "", log.Lmicroseconds)
	Info = log.New(ioutil.Discard, "", log.Lmicroseconds)
}

func (kv *KVPaxos) waitForCommit(seq int, op Op) {
	kv.px.Start(seq, op)

	to := 10 * time.Millisecond
	for {
		decided, v := kv.px.Status(seq)
		if decided {
			dop := v.(Op)

			kv.mu.Lock()
			defer kv.mu.Unlock()

			if reqState, ok := kv.requests[dop.Id]; ok {
				Trace.Printf("[%d] [waitForCommit] - Committed [%d] at [%d]\n", kv.me, dop.Id, seq)
				reqState.state = Committed
				reqState.seq = seq
			} else {
				Trace.Printf("[%d] [*waitForCommit*] - Committed [%d] at [%d]\n", kv.me, dop.Id, seq)
				kv.requests[dop.Id] = &RequestState{
					xid:   dop.Id,
					state: Committed,
					seq:   seq,
				}
			}

			return
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) requestExists(op Op, responseChan chan interface{}) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reqState, ok := kv.requests[op.Id]; ok {
		Trace.Printf("[%d] [mustCommit] [%d] - Request exists with state [%s]\n", kv.me, op.Id, reqState.state)

		if reqState.state != Executed {
			reqState.waiting = append(reqState.waiting, responseChan)
			return true
		}

		kv.subscribe <- ExecuteRequest{seq: reqState.seq, responseChan: responseChan}
		return true
	}

	Trace.Printf("[%d] [*mustCommit*] [%d] - New request\n", kv.me, op.Id)

	kv.requests[op.Id] = &RequestState{
		xid:     op.Id,
		state:   Active,
		waiting: []chan interface{}{responseChan},
	}

	return false
}

func (kv *KVPaxos) mustCommit(op Op, responseChan chan interface{}) {
	if kv.requestExists(op, responseChan) {
		return
	}

	for {
		seq := kv.px.Max() + 1
		kv.waitForCommit(seq, op)

		kv.mu.Lock()
		state := kv.requests[op.Id].state
		commitedSeq := kv.requests[op.Id].seq
		kv.mu.Unlock()

		if state != Committed {
			continue
		}

		responseChan := make(chan interface{})

		kv.subscribe <- ExecuteRequest{seq: commitedSeq, responseChan: responseChan}

		resp := <-responseChan

		kv.mu.Lock()
		reqState := kv.requests[op.Id]

		for _, waitingRequest := range reqState.waiting {
			waitingRequest <- resp
		}

		reqState.state = Executed

		Trace.Printf("[%d] [mustCommit] [%d] - Executed request at seq [%d]\n", kv.me, op.Id, commitedSeq)

		kv.mu.Unlock()
		return
	}
}

func (kv *KVPaxos) executeStateMachine() {
	var i int // current position on log

	data := make(map[string]string)

	responses := make(map[int]interface{})

	for !kv.dead {
		req := <-kv.subscribe

		Trace.Printf("[%d] [SM] - Subscribe [%d]\n", kv.me, req.seq)

		if resp, ok := responses[req.seq]; ok {
			Trace.Printf("[%d] [SM] - Early Notify [%d]\n", kv.me, req.seq)
			req.responseChan <- resp
			continue
		}

		for ; i <= req.seq; i++ {
			Trace.Printf("[%d] [SM] - Processing [%d]\n", kv.me, i)
			decided, v := kv.px.Status(i)
			if !decided {
				kv.waitForCommit(i, Op{Id: nrand(), Type: Nop})
				_, v = kv.px.Status(i)
			}

			op := v.(Op)

			switch op.Type {
			case Put:
				reply := PutReply{PreviousValue: data[op.Key]}
				data[op.Key] = op.Value
				responses[i] = reply
				Info.Printf("[%d] [SM] [%d] - Put [%s] : [%s]\n", kv.me, i, op.Key, op.Value)
			case PutHash:
				reply := PutReply{PreviousValue: data[op.Key]}
				data[op.Key] = strconv.Itoa(int(hash(data[op.Key] + op.Value)))
				responses[i] = reply
				Info.Printf("[%d] [SM] [%d] - PutHash [%s] : [%s] -> [%s] -> [%s]\n", 
					kv.me, i, op.Key, op.Value, data[op.Key], reply.PreviousValue)
			case Get:
				reply := GetReply{Value: data[op.Key]}
				responses[i] = reply
				Info.Printf("[%d] [SM] [%d] - Get [%s] -> [%s]\n", kv.me, i, op.Key, reply.Value)
			}
		}

		// kv.px.Done(req.seq)

		Trace.Printf("[%d] [SM] - Notify [%d]\n", kv.me, req.seq)
		req.responseChan <- responses[req.seq]
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	Info.Printf("[%d] *Get* [%d] - Key [%s]\n", kv.me, args.Xid, args.Key)

	op := Op{Id: args.Xid, Type: Get, Key: args.Key}
	responseChan := make(chan interface{})

	go kv.mustCommit(op, responseChan)

	resp := <-responseChan

	*reply = resp.(GetReply)

	if reply.Value == "" {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	Info.Printf("[%d] *Put* [%d] - Key [%s] Value [%s] DoHash [%t]\n", kv.me, args.Xid, args.Key, args.Value, args.DoHash)

	op := Op{Id: args.Xid, Key: args.Key, Value: args.Value}

	if args.DoHash {
		op.Type = PutHash
	} else {
		op.Type = Put
	}

	responseChan := make(chan interface{})

	go kv.mustCommit(op, responseChan)

	resp := <-responseChan

	*reply = resp.(PutReply)

	reply.Err = OK

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.requests = make(map[int64]*RequestState)
	kv.subscribe = make(chan ExecuteRequest)

	go kv.executeStateMachine()

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
