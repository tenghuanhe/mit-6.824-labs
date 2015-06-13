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
import "strings"

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
	Xid     int64
	State   string
	seq     int
	waiting []chan interface{}
	Resp    interface{}
}

type ConfigBid struct {
	Config shardmaster.Config
	Bid    int
}

type StartHandoff struct {
	Id     ConfigBid
	Leader int
}

type EndHandoff struct {
	Id     ConfigBid
	Shards []*ShardData
}

type Nop int

type ShardData struct {
	mu       sync.Mutex
	active   bool
	Id       int
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
	cond           *sync.Cond
	dataForShard   [shardmaster.NShards + 1]*ShardData
	config         shardmaster.Config
	subscribe      chan ExecuteRequest
	commitRequest  chan Op
	commitResponse chan int
	ping           chan PingArgs
}

func init() {
	log.SetOutput(ioutil.Discard)
	Trace = log.New(os.Stdout, "", log.Lmicroseconds)
	Info = log.New(os.Stdout, "", log.Lmicroseconds)
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
			reqState.State = Committed
			reqState.seq = seq
		} else {
			tablet.Requests[dop.Id] = &RequestState{
				Xid:   dop.Id,
				State: Committed,
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

	if !(tablet.active || op.Shard == shardmaster.NShards) {
		responseChan <- getWrongGroupReply(op)
		return true
	}

	if reqState, ok := tablet.Requests[op.Id]; ok {
		Trace.Printf("[[%d] [%d] [mustCommit] [%d] - Request exists with state [%s]\n", kv.gid, kv.me, op.Id, reqState.State)

		if reqState.State != Executed {
			reqState.waiting = append(reqState.waiting, responseChan)
			return true
		}

		responseChan <- reqState.Resp

		return true
	}

	Trace.Printf("[%d] [%d] [*mustCommit*] [%d] - New request\n", kv.gid, kv.me, op.Id)

	tablet.Requests[op.Id] = &RequestState{
		Xid:     op.Id,
		State:   Active,
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
		if reqState.State == Committed {
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

func (kv *ShardKV) executeStateMachine() {
	var i int // current position on log

	log := make([]MapEntry, 0)

	for !kv.dead {
		req := <-kv.subscribe
		Trace.Printf("[%d] [%d] [SM] - Subscribe [%d]\n", kv.gid, kv.me, req.seq)

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
			case *StartHandoff:
				kv.startHandoff(i, op)
			case *EndHandoff:
				kv.endHandoff(i, op)
			}

			log = append(log, MapEntry{op.Shard, op.Id})
		}

		Trace.Printf("[%d] [%d] [SM] - Notify [%d]\n", kv.gid, kv.me, req.seq)

		e := log[len(log)-1]
		req.responseChan <- kv.dataForShard[e.num].Requests[e.xid].Resp
	}
}

///////////////////////////// Get, Put & PutHash ////////////////////////////////////////////////////////////

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
		tablet.Requests[op.Id].State = Executed
	} else {
		reply = getWrongGroupReply(op).(PutReply)
	}

	tablet.Requests[op.Id].Resp = reply
}

func (kv *ShardKV) get(i int, op Op) {
	args := op.Args.(*GetArgs)
	tablet := kv.dataForShard[op.Shard]

	tablet.mu.Lock()
	defer tablet.mu.Unlock()

	var reply GetReply
	if tablet.active {
		reply = GetReply{Value: tablet.Data[args.Key]}
		Trace.Printf("[%d] [%d] [SM] [%d] - Get [%s] -> [%s]\n", kv.gid, kv.me, i, args.Key, reply.Value)
		tablet.Requests[op.Id].State = Executed
	} else {
		reply = getWrongGroupReply(op).(GetReply)
	}

	tablet.Requests[op.Id].Resp = reply
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

///////////////////////////// Reconfiguration & Handoff /////////////////////////////////////////////////////

func (kv *ShardKV) prettyPrintConfig(h string, current *shardmaster.Config, latest *shardmaster.Config) {
	state := make([]string, shardmaster.NShards)

	for i := range current.Shards {
		if current.Shards[i] == kv.gid {
			if latest.Shards[i] != kv.gid {
				state[i] = fmt.Sprintf("\033[1m\033[31m%d\033[0m", i)
			} else {
				state[i] = fmt.Sprintf("%d", i)
			}
		} else {
			if latest.Shards[i] == kv.gid {
				state[i] = fmt.Sprintf("\033[1m\033[32m%d\033[0m", i)
			} else {
				state[i] = "."
			}
		}
	}

	Info.Printf("[%s] [%d] [%d] [%d] [%s]\n", h, kv.gid, kv.me, latest.Num, strings.Join(state, " "))
}

func (kv *ShardKV) Ping(args *PingArgs, reply *PingReply) error {
	Trace.Printf("[%d] [%d] *Ping* - From [%d] Num [%d] Bid [%d]\n", kv.gid, kv.me, args.Me, args.Num, args.Bid)

	metaTablet := kv.dataForShard[shardmaster.NShards]

	if !metaTablet.active {
		kv.ping <- *args
	}

	return nil
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	Trace.Printf("[%d] [%d] *GetShard* - Shard [%d] Num [%d]\n", kv.gid, kv.me, args.Shard, args.Num)

	tablet := kv.dataForShard[args.Shard]

	tablet.mu.Lock()
	defer tablet.mu.Unlock()

	if !tablet.active && tablet.Num == args.Num {
		reply.Data = *tablet
		reply.Err = OK

		for i, r := range reply.Data.Requests {
			if r.State != Executed {
				delete(reply.Data.Requests, i)
			}
		}

		return nil
	}

	reply.Err = ErrNotFound

	return nil
}

type ShardState struct {
	Shard int
	State bool
}

func (kv *ShardKV) isHandoffRequired(latest *shardmaster.Config) (result []ShardState) {
	result = make([]ShardState, 0)

	current := &kv.config

	if current.Num == latest.Num {
		return
	}

	for i := range current.Shards {
		if current.Shards[i] == kv.gid && latest.Shards[i] != kv.gid {
			result = append(result, ShardState{i, false})
		}
		if current.Shards[i] != kv.gid && latest.Shards[i] == kv.gid {
			result = append(result, ShardState{i, true})
		}
	}

	return
}

func (kv *ShardKV) startHandoff(i int, op Op) {
	args := op.Args.(*StartHandoff)

	Trace.Printf("[%d] [%d] [SM] [%d] - Start Handoff Num [%d] Bid [%d] Leader [%d]\n",
		kv.gid, kv.me, i, args.Id.Config.Num, args.Id.Bid, args.Leader)

	metaTablet := kv.dataForShard[shardmaster.NShards]

	metaTablet.mu.Lock()
	defer metaTablet.mu.Unlock()

	for metaTablet.active {
		kv.cond.Wait()
	}

	shardsToBeMoved := kv.isHandoffRequired(&args.Id.Config)

	for _, ss := range shardsToBeMoved {
		if !ss.State {
			tablet := kv.dataForShard[ss.Shard]
			tablet.mu.Lock()
			tablet.active = false
			tablet.Num = args.Id.Config.Num
			tablet.mu.Unlock()
		}
	}

	if kv.me == args.Leader {
		go kv.leaderStartHandoff(&args.Id.Config, args.Id.Bid, shardsToBeMoved)
	} else {
		go kv.followerStartHandoff(&args.Id.Config, args.Id.Bid, args.Leader)
	}
}

func (kv *ShardKV) leaderStartHandoff(latest *shardmaster.Config, bid int, shardsToBeMoved []ShardState) {
	go func() {
		metaTablet := kv.dataForShard[shardmaster.NShards]

		for !metaTablet.active {
			for i, srv := range latest.Groups[kv.gid] {
				if i != kv.me {
					args := &PingArgs{Me: kv.me, Num: latest.Num, Bid: bid}
					var reply PingReply
					call(srv, "ShardKV.Ping", args, &reply)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	kv.fetchShards(latest, shardsToBeMoved, bid)
}

func (kv *ShardKV) followerStartHandoff(latest *shardmaster.Config, bid int, leader int) {
	metaTablet := kv.dataForShard[shardmaster.NShards]

	d := DeadPings*PingInterval + time.Duration(rand.Intn(100))*time.Millisecond

	Trace.Printf("[%d] [%d] [Follower Start Handoff] - Num [%d] Bid [%d] Leader [%d] Timeout [%v]\n",
		kv.gid, kv.me, latest.Num, bid, leader, d)

	timer := time.NewTimer(d)

loop:
	for !metaTablet.active {
		select {
		case args := <-kv.ping:
			if args.Me == leader && args.Num == latest.Num && args.Bid == bid {
				timer.Reset(d)
			}
		case <-timer.C:
			timer.Stop()
			break loop
		}
	}

	if metaTablet.active {
		return
	}

	kv.reconfigure(latest, bid+1)
}

func (kv *ShardKV) fetchShards(latest *shardmaster.Config, shardsToBeMoved []ShardState, bid int) {
	var l sync.Mutex
	localStorage := make([]*ShardData, 0)

	current := &kv.config

	done := make(chan struct{})

	getShardsFromCurrentOwner := func(shard int, num int, gid int64) {
		Trace.Printf("[%d] [%d] [Fetch Shards] - Shard [%d] Num [%d] From GID [%d]\n",
			kv.gid, kv.me, shard, num, gid)

		servers := current.Groups[gid]

		addToLocalStorage := func(shardData *ShardData) {
			l.Lock()
			localStorage = append(localStorage, shardData)
			l.Unlock()
			done <- struct{}{}
		}

		if len(servers) == 0 {
			addToLocalStorage(&ShardData{
				Id:       shard,
				Data:     make(map[string]string),
				Requests: make(map[int64]*RequestState),
			})
			return
		}

		for {
			for _, srv := range servers {
				args := &GetShardArgs{Shard: shard, Num: num}

				var reply GetShardReply
				ok := call(srv, "ShardKV.GetShard", args, &reply)
				if ok && reply.Err == OK {
					addToLocalStorage(&reply.Data)
					return
				}

			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	count := 0
	for _, ss := range shardsToBeMoved {
		if ss.State {
			count++
			go getShardsFromCurrentOwner(ss.Shard, latest.Num, current.Shards[ss.Shard])
		}
	}

	for count > 0 {
		<-done
		count--
	}

	responseChan := make(chan interface{})

	configBid := ConfigBid{Config: *latest, Bid: bid + 1}

	args := EndHandoff{Id: configBid, Shards: localStorage}

	xid := hashConfigBid(&configBid)

	Trace.Printf("[%d] [%d] *End Handoff* [%d] - Num [%d]\n", kv.gid, kv.me, xid, args.Id.Config.Num)

	go kv.mustCommit(Op{Id: xid, Args: &args, Shard: shardmaster.NShards}, responseChan)

	<-responseChan
}

func (kv *ShardKV) endHandoff(i int, op Op) {
	args := op.Args.(*EndHandoff)

	Trace.Printf("[%d] [%d] [SM] [%d] - End Handoff Num [%d] Shards Moved [%d]\n",
		kv.gid, kv.me, i, args.Id.Config.Num, len(args.Shards))

	metaTablet := kv.dataForShard[shardmaster.NShards]

	metaTablet.mu.Lock()
	defer metaTablet.mu.Unlock()

	for _, shardData := range args.Shards {
		tablet := kv.dataForShard[shardData.Id]
		tablet.mu.Lock()
		tablet.active = true
		tablet.Num = args.Id.Config.Num
		tablet.Data = shardData.Data
		tablet.Requests = shardData.Requests
		tablet.mu.Unlock()
	}

	metaTablet.active = true
	metaTablet.Num = args.Id.Config.Num + 1

	kv.config = args.Id.Config
}

func (kv *ShardKV) reconfigure(latest *shardmaster.Config, bid int) {
	responseChan := make(chan interface{})

	configBid := ConfigBid{Config: *latest, Bid: bid}

	args := StartHandoff{Id: configBid, Leader: kv.me}

	xid := hashConfigBid(&configBid)

	Trace.Printf("[%d] [%d] *Start Handoff* [%d] - Num [%d] Bid [%d]\n",
		kv.gid, kv.me, xid, args.Id.Config.Num, args.Id.Bid)

	go kv.mustCommit(Op{Id: xid, Args: &args, Shard: shardmaster.NShards}, responseChan)

	<-responseChan
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	metaTablet := kv.dataForShard[shardmaster.NShards]

	metaTablet.mu.Lock()
	defer metaTablet.mu.Unlock()

	if metaTablet.active {
		latest := kv.sm.Query(metaTablet.Num)
		if len(kv.isHandoffRequired(&latest)) > 0 {
			metaTablet.active = false
			kv.prettyPrintConfig("Reconfigure", &kv.config, &latest)
			go kv.reconfigure(&latest, 0)
			kv.cond.Signal()
		} else {
			kv.config = latest
			metaTablet.Num = latest.Num + 1
		}
	}
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
	gob.Register(&RequestState{})
	gob.Register(&ConfigBid{})
	gob.Register(&StartHandoff{})
	gob.Register(&EndHandoff{})
	gob.Register(&ShardData{})
	gob.Register(&PutArgs{})
	gob.Register(&GetArgs{})
	gob.Register(&GetShardArgs{})
	gob.Register(&PingArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	for i := range kv.dataForShard {
		kv.dataForShard[i] = &ShardData{
			Id:       i,
			Data:     make(map[string]string),
			Requests: make(map[int64]*RequestState),
		}
	}

	kv.cond = sync.NewCond(&kv.dataForShard[shardmaster.NShards].mu)

	kv.dataForShard[shardmaster.NShards].active = true
	kv.dataForShard[shardmaster.NShards].Num = -1

	kv.subscribe = make(chan ExecuteRequest)
	kv.commitRequest = make(chan Op)
	kv.commitResponse = make(chan int)
	kv.ping = make(chan PingArgs)

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
