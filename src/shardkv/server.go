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

type StartHandoff struct {
	Num int
	Bid int
}

type EndHandoff int

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

	tablet := kv.dataForShard[op.Shard]

	flushResponse := func(resp interface{}) {
		for _, waitingRequest := range tablet.Requests[op.Id].waiting {
			waitingRequest <- resp
		}
	}

	if seq == -1 {
		flushResponse(getWrongGroupReply(op))
		return
	}

	executeResponseChan := make(chan interface{})

	kv.subscribe <- ExecuteRequest{seq: <-kv.commitResponse, responseChan: executeResponseChan}

	resp := <-executeResponseChan

	tablet.mu.Lock()
	defer tablet.mu.Unlock()

	flushResponse(resp)

	tablet.Requests[op.Id].state = Executed
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

	// Your initialization code here.
	// Don't call Join().

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
