package shardmaster

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
import crand "crypto/rand"
import "math/big"
import "time"
import "io/ioutil"
import "sort"

var (
	Trace *log.Logger
	Info  *log.Logger
)

type Op struct {
	Id   int64
	Args interface{}
}

type ExecuteRequest struct {
	seq          int
	num          int
	responseChan chan interface{}
}

type ShardMaster struct {
	mu                       sync.Mutex
	l                        net.Listener
	me                       int
	dead                     bool // for testing
	unreliable               bool // for testing
	px                       *paxos.Paxos
	configs                  []Config // indexed by config num
	subscribe                chan ExecuteRequest
	commitRequestsInputChan  chan Op
	commitRequestsOutputChan chan int
}

func init() {
	log.SetOutput(ioutil.Discard)
	Trace = log.New(ioutil.Discard, "", log.Lmicroseconds)
	Info = log.New(os.Stdout, "", log.Lmicroseconds)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func (sm *ShardMaster) isInstanceDecided(seq int) bool {
	decided, _ := sm.px.Status(seq)
	return decided
}

func (sm *ShardMaster) waitForCommit(seq int, op Op) {
	sm.px.Start(seq, op)

	to := 10 * time.Millisecond
	for {
		if sm.isInstanceDecided(seq) {
			return
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) mustCommit(op Op, responseChan chan interface{}) {
	sm.commitRequestsInputChan <- op

	seq := <-sm.commitRequestsOutputChan

	switch op.Args.(type) {
	case *QueryArgs:
		queryArgs := op.Args.(*QueryArgs)
		sm.subscribe <- ExecuteRequest{seq: seq, num: queryArgs.Num, responseChan: responseChan}
	default:
		responseChan <- struct{}{}
	}
}

func (sm *ShardMaster) commitRequests() {
	var firstUnchosenIndex int

nextRequest:
	for !sm.dead {
		op := <-sm.commitRequestsInputChan

		Trace.Printf("[%d] [CR] - Received [%d]\n", sm.me, op.Id)

		for ; sm.isInstanceDecided(firstUnchosenIndex); firstUnchosenIndex++ {
		}

		for {
			Trace.Printf("[%d] [CR] - Trying to commit [%d] at [%d]\n", sm.me, op.Id, firstUnchosenIndex)

			sm.waitForCommit(firstUnchosenIndex, op)
			firstUnchosenIndex++

			seq := firstUnchosenIndex - 1

			_, v := sm.px.Status(seq)

			dop := v.(Op)

			if dop.Id == op.Id {
				Trace.Printf("[%d] [CR] [%d] - Committed at [%d]\n", sm.me, op.Id, seq)
				sm.commitRequestsOutputChan <- seq
				continue nextRequest
			}
		}
	}
}

type CountElem struct {
	gid     int64
	current int
	desired int
}

type ByCurrent []CountElem

func (a ByCurrent) Len() int           { return len(a) }
func (a ByCurrent) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByCurrent) Less(i, j int) bool { return a[i].current < a[j].current }

type ByDeficit []CountElem

func (a ByDeficit) Len() int           { return len(a) }
func (a ByDeficit) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDeficit) Less(i, j int) bool { return a[i].current-a[i].desired < a[j].current-a[j].desired }

func optimalConfiguration(Nshards int, Kservers int) []int {
	conf := make([]int, Kservers)

	for i := Kservers - 1; i >= 0; i-- {
		conf[i] = Nshards / Kservers
		Nshards -= conf[i]
		Kservers -= 1
	}

	return conf
}

func shuffle(shards [NShards]int64, gid int64) [NShards]int64 {
	Trace.Printf("[shuffle] - Received shards: %v gid: [%d]\n", shards, gid)

	counts := make(map[int64]*CountElem)

	for _, g := range shards {
		if g > 0 {
			if counts[g] == nil {
				counts[g] = &CountElem{gid: g}
			}
			counts[g].current++
		}
	}

	if len(counts) == 0 {
		newShards := shards

		for i := range newShards {
			newShards[i] = gid
		}

		return newShards
	}

	countsList := make([]CountElem, 0)
	for g, elem := range counts {
		if g != gid {
			countsList = append(countsList, CountElem{gid: g, current: elem.current})
		}
	}

	sort.Sort(ByCurrent(countsList))

	var optimal []int

	if elem, ok := counts[gid]; !ok {
		countsList = append(countsList, CountElem{gid: gid, current: 0})
		counts[gid] = &CountElem{gid: gid}
		optimal = optimalConfiguration(NShards, len(countsList))
	} else {
		countsList = append(countsList, CountElem{gid: gid, current: elem.current})
		optimal = optimalConfiguration(NShards, len(countsList)-1)
		optimal = append(optimal, 0)
	}

	for i, cnt := range optimal {
		countsList[i].desired = cnt
		counts[countsList[i].gid].desired = cnt
	}

	Trace.Printf("[shuffle] - countsList: %v\n", countsList)

	sort.Sort(sort.Reverse(ByDeficit(countsList)))

	Trace.Printf("[shuffle] - ByDeficit(countsList): %v\n", countsList)

	curr := len(countsList) - 1

	newShards := shards

	for i, g := range shards {
		if counts[g].desired > 0 {
			counts[g].desired--
		} else {
			newShards[i] = countsList[curr].gid
			counts[shards[i]].desired--
			if counts[shards[i]].desired == 0 {
				curr--
			}
		}
	}

	return newShards
}

func copyGroups(groups map[int64][]string) map[int64][]string {
	newGroups := make(map[int64][]string)

	for k, v := range groups {
		copyServers := make([]string, len(v))
		copy(copyServers, v)
		newGroups[k] = copyServers
	}

	return newGroups
}

func (sm *ShardMaster) join(args *JoinArgs) {
	if _, ok := sm.configs[len(sm.configs)-1].Groups[args.GID]; ok {
		return
	}

	newGroups := copyGroups(sm.configs[len(sm.configs)-1].Groups)

	copyServers := make([]string, len(args.Servers))

	copy(copyServers, args.Servers)

	newGroups[args.GID] = copyServers

	newConfig := Config{
		Num:    sm.configs[len(sm.configs)-1].Num + 1,
		Shards: shuffle(sm.configs[len(sm.configs)-1].Shards, args.GID),
		Groups: newGroups,
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) leave(args *LeaveArgs) {
	if _, ok := sm.configs[len(sm.configs)-1].Groups[args.GID]; !ok {
		return
	}

	newGroups := copyGroups(sm.configs[len(sm.configs)-1].Groups)

	delete(newGroups, args.GID)

	newConfig := Config{
		Num:    sm.configs[len(sm.configs)-1].Num + 1,
		Shards: shuffle(sm.configs[len(sm.configs)-1].Shards, args.GID),
		Groups: newGroups,
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) move(args *MoveArgs) {
	newShards := sm.configs[len(sm.configs)-1].Shards

	newShards[args.Shard] = args.GID

	newGroups := copyGroups(sm.configs[len(sm.configs)-1].Groups)

	newConfig := Config{
		Num:    sm.configs[len(sm.configs)-1].Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) executeStateMachine() {
	var i int // current position on log

	for !sm.dead {
		req := <-sm.subscribe
		Trace.Printf("[%d] [SM] - Subscribe [%d]\n", sm.me, req.seq)

		if req.num >= 0 && req.num < len(sm.configs) {
			Trace.Printf("[%d] [SM] - Early Notify Num: [%d] at Seq:[%d]\n", sm.me, req.num, req.seq)
			req.responseChan <- sm.configs[req.num]
			continue
		}

		for ; i <= req.seq; i++ {
			Trace.Printf("[%d] [SM] - Processing [%d]\n", sm.me, i)

			if !sm.isInstanceDecided(i) {
				sm.waitForCommit(i, Op{Id: nrand()})
			}

			_, v := sm.px.Status(i)

			op := v.(Op)

			switch op.Args.(type) {
			case *JoinArgs:
				args := op.Args.(*JoinArgs)
				sm.join(args)
			case *LeaveArgs:
				args := op.Args.(*LeaveArgs)
				sm.leave(args)
			case *MoveArgs:
				args := op.Args.(*MoveArgs)
				sm.move(args)
			}

			Info.Printf("[%d] [SM] - Seq: [%d] Config: %v\n", sm.me, i, sm.configs[len(sm.configs)-1].Shards)
		}

		Trace.Printf("[%d] [SM] - Notify Num: [%d] at Seq:[%d]\n", sm.me, req.num, req.seq)
		req.responseChan <- sm.configs[len(sm.configs)-1]
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	id := nrand()

	Info.Printf("[%d] *Join* [%d] - GID [%d]\n", sm.me, id, args.GID)

	responseChan := make(chan interface{})

	go sm.mustCommit(Op{Id: id, Args: args}, responseChan)

	<-responseChan

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	id := nrand()

	Info.Printf("[%d] *Leave* [%d] - GID [%d]\n", sm.me, id, args.GID)

	responseChan := make(chan interface{})

	go sm.mustCommit(Op{Id: id, Args: args}, responseChan)

	<-responseChan

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	id := nrand()

	Info.Printf("[%d] *Move* [%d] - Shard: [%d] to GID: [%d]\n", sm.me, id, args.Shard, args.GID)

	responseChan := make(chan interface{})

	go sm.mustCommit(Op{Id: id, Args: args}, responseChan)

	<-responseChan

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	id := nrand()

	Info.Printf("[%d] *Query* [%d] - Num [%d]\n", sm.me, id, args.Num)

	responseChan := make(chan interface{})

	go sm.mustCommit(Op{Id: id, Args: args}, responseChan)

	resp := <-responseChan

	reply.Config = resp.(Config)

	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.subscribe = make(chan ExecuteRequest)
	sm.commitRequestsInputChan = make(chan Op)
	sm.commitRequestsOutputChan = make(chan int)

	go sm.commitRequests()

	go sm.executeStateMachine()

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
