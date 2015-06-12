package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
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
	Active    = "Active"
	Committed = "Committed"
	Executed  = "Executed"
)

type ExecuteRequest struct {
	seq          int
	responseChan chan interface{}
}

type RequestState struct {
	xid     int64
	state   string
	seq     int
	waiting []chan interface{}
	resp    interface{}
}

type StartHandoff struct {
	Num int
	Bid int
}

type EndHandoff int

type Nop int

type ShardData struct {
	mu       sync.Mutex
	active   bool
	Num      int
	Data     map[string]string
	Requests map[int64]*RequestState
}

type Op struct {
	Id    int64
	Args  interface{}
	Shard int
}

type ShardKV struct {
	mu             sync.Mutex
	l              net.Listener
	me             int
	dead           bool // for testing
	unreliable     bool // for testing
	sm             *shardmaster.Clerk
	px             *paxos.Paxos
	gid            int64 // my replica group ID
	dataForShard   [shardmaster.NShards]*ShardData
	subscribe      chan ExecuteRequest
	commitRequest  chan Op
	commitResponse chan int
}

func init() {
	log.SetOutput(ioutil.Discard)
	Trace = log.New(ioutil.Discard, "", log.Lmicroseconds)
	Info = log.New(ioutil.Discard, "", log.Lmicroseconds)
}

func getWrongGroupReply(op Op) (reply interface{}) {
	switch op.Args.(type) {
	case *GetArgs:
		reply = GetReply{Err: ErrWrongGroup}
	case *PutArgs:
		reply = PutReply{Err: ErrWrongGroup}
	}

	return
}

func (kv *ShardKV) isInstanceDecided(seq int) bool {
	decided, v := kv.px.Status(seq)
	if decided {
		dop := v.(Op)

		tablet := kv.dataForShard[dop.Shard]

		tablet.mu.Lock()
		defer tablet.mu.Unlock()

		if reqState, ok := tablet.Requests[dop.Id]; ok {
			reqState.state = Committed
			reqState.seq = seq
		} else {
			tablet.Requests[dop.Id] = &RequestState{
				xid:   dop.Id,
				state: Committed,
				seq:   seq,
			}
		}
		return true
	}
	return false
}

func (kv *ShardKV) waitForCommit(seq int, op Op) {
	kv.px.Start(seq, op)

	to := 10 * time.Millisecond
	for {
		if kv.isInstanceDecided(seq) {
			return
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *ShardKV) requestExists(op Op, responseChan chan interface{}) bool {
	tablet := kv.dataForShard[op.Shard]

	tablet.mu.Lock()
	defer tablet.mu.Unlock()

	if !tablet.active {
		responseChan <- getWrongGroupReply(op)
		return true
	}

	if reqState, ok := tablet.Requests[op.Id]; ok {
		Trace.Printf("[[%d] [%d] [mustCommit] [%d] - Request exists with state [%s]\n", kv.gid, kv.me, op.Id, reqState.state)

		if reqState.state != Executed {
			reqState.waiting = append(reqState.waiting, responseChan)
			return true
		}

		kv.subscribe <- ExecuteRequest{seq: reqState.seq, responseChan: responseChan}
		return true
	}

	Trace.Printf("[%d] [%d] [*mustCommit*] [%d] - New request\n", kv.gid, kv.me, op.Id)

	tablet.Requests[op.Id] = &RequestState{
		xid:     op.Id,
		state:   Active,
		waiting: []chan interface{}{responseChan},
	}

	return false
}

func (kv *ShardKV) mustCommit(op Op, responseChan chan interface{}) {
	if kv.requestExists(op, responseChan) {
		return
	}

	kv.commitRequest <- op

	seq := <-kv.commitResponse

	flushResponse := func(resp interface{}) {
		tablet := kv.dataForShard[op.Shard]

		tablet.mu.Lock()
		defer tablet.mu.Unlock()

		for _, waitingRequest := range tablet.Requests[op.Id].waiting {
			waitingRequest <- resp
		}
	}

	if seq == -1 {
		flushResponse(getWrongGroupReply(op))
		return
	}

	executeResponseChan := make(chan interface{})

	kv.subscribe <- ExecuteRequest{seq: seq, responseChan: executeResponseChan}

	flushResponse(<-executeResponseChan)
}

func (kv *ShardKV) commitRequests() {
	var firstUnchosenIndex int

	isRequestCommitted := func(num int, xid int64) int {
		tablet := kv.dataForShard[num]

		tablet.mu.Lock()
		defer tablet.mu.Unlock()

		reqState, _ := tablet.Requests[xid]
		if reqState.state == Committed {
			Trace.Printf("[%d] [%d] [CR] [%d] - Committed at [%d]\n", kv.gid, kv.me, xid, reqState.seq)
			return reqState.seq
		}

		return -1
	}

nextRequest:
	for !kv.dead {
		op := <-kv.commitRequest

		Trace.Printf("[%d] [%d] [CR] - Received [%d]\n", kv.gid, kv.me, op.Id)

		if seq := isRequestCommitted(op.Shard, op.Id); seq != -1 {
			kv.commitResponse <- seq
			continue nextRequest
		}

		for kv.isInstanceDecided(firstUnchosenIndex) {
			firstUnchosenIndex++
			if seq := isRequestCommitted(op.Shard, op.Id); seq != -1 {
				kv.commitResponse <- seq
				continue nextRequest
			}
		}

		for {
			Trace.Printf("[%d] [%d] [CR] - Trying to commit [%d] at [%d]\n", kv.gid, kv.me, op.Id, firstUnchosenIndex)

			kv.waitForCommit(firstUnchosenIndex, op)

			firstUnchosenIndex++

			if seq := isRequestCommitted(op.Shard, op.Id); seq != -1 {
				kv.commitResponse <- seq
				continue nextRequest
			}
		}
	}
}

type MapEntry struct {
	num int
	xid int64
}

func (kv *ShardKV) put(i int, op Op) {
	args := op.Args.(*PutArgs)
	tablet := kv.dataForShard[op.Shard]

	tablet.mu.Lock()
	defer tablet.mu.Unlock()

	var reply PutReply
	if tablet.active {
		reply = PutReply{PreviousValue: tablet.Data[args.Key]}
		if args.DoHash {
			tablet.Data[args.Key] = strconv.Itoa(int(hash(tablet.Data[args.Key] + args.Value)))
			Trace.Printf("[%d] [%d] [SM] [%d] - PutHash [%s] : [%s] -> [%s] -> [%s]\n",
				kv.gid, kv.me, i, args.Key, args.Value, tablet.Data[args.Key], reply.PreviousValue)
		} else {
			tablet.Data[args.Key] = args.Value
			Trace.Printf("[%d] [%d] [SM] [%d] - Put [%s] : [%s]\n", kv.gid, kv.me, i, args.Key, args.Value)
		}
		tablet.Requests[op.Id].state = Executed
	} else {
		reply = getWrongGroupReply(op).(PutReply)
	}

	tablet.Requests[op.Id].resp = reply
}

func (kv *ShardKV) get(i int, op Op) {
	args := op.Args.(*GetArgs)
	tablet := kv.dataForShard[op.Shard]

	tablet.mu.Lock()
	defer tablet.mu.Unlock()

	var reply GetReply
	if tablet.active {
		reply = GetReply{Value: tablet.Data[args.Key]}
		tablet.Requests[op.Id].state = Executed
	} else {
		reply = getWrongGroupReply(op).(GetReply)
	}

	tablet.Requests[op.Id].resp = reply
}

func (kv *ShardKV) executeStateMachine() {
	var i int // current position on log

	log := make([]MapEntry, 0)

	for !kv.dead {
		req := <-kv.subscribe
		Trace.Printf("[%d] [%d] [SM] - Subscribe [%d]\n", kv.gid, kv.me, req.seq)

		if req.seq < len(log) {
			Trace.Printf("[%d] [%d] [SM] - Early Notify [%d]\n", kv.gid, kv.me, req.seq)
			e := log[req.seq]
			req.responseChan <- kv.dataForShard[e.num].Requests[e.xid].resp
			continue
		}

		for ; i <= req.seq; i++ {
			Trace.Printf("[%d] [%d] [SM] - Processing [%d]\n", kv.gid, kv.me, i)

			if !kv.isInstanceDecided(i) {
				kv.waitForCommit(i, Op{Id: nrand(), Args: Nop(0), Shard: -1})
			}

			_, v := kv.px.Status(i)

			op := v.(Op)

			switch op.Args.(type) {
			case *PutArgs:
				kv.put(i, op)
			case *GetArgs:
				kv.get(i, op)
			}

			log = append(log, MapEntry{op.Shard, op.Id})
		}

		Trace.Printf("[%d] [%d] [SM] - Notify [%d]\n", kv.gid, kv.me, req.seq)

		e := log[len(log)-1]
		req.responseChan <- kv.dataForShard[e.num].Requests[e.xid].resp
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	Trace.Printf("[%d] [%d] *Get* [%d] - Key [%s]\n", kv.gid, kv.me, args.Xid, args.Key)

	responseChan := make(chan interface{})

	go kv.mustCommit(Op{Id: args.Xid, Args: args, Shard: key2shard(args.Key)}, responseChan)

	resp := <-responseChan

	*reply = resp.(GetReply)

	if reply.Err == "" {
		if reply.Value == "" {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
		}
	}

	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	Trace.Printf("[%d] [%d] *Put* [%d] - Key [%s] Value [%s] DoHash [%t]\n",
		kv.gid, kv.me, args.Xid, args.Key, args.Value, args.DoHash)

	responseChan := make(chan interface{})

	go kv.mustCommit(Op{Id: args.Xid, Args: args, Shard: key2shard(args.Key)}, responseChan)

	resp := <-responseChan

	*reply = resp.(PutReply)

	if reply.Err == "" {
		reply.Err = OK
	}

	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	for i := range kv.dataForShard {
		kv.dataForShard[i] = &ShardData{
			Data:     make(map[string]string),
			Requests: make(map[int64]*RequestState),
		}
	}

	kv.subscribe = make(chan ExecuteRequest)
	kv.commitRequest = make(chan Op)
	kv.commitResponse = make(chan int)

	go kv.commitRequests()

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
