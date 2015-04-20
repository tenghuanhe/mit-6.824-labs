package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "io/ioutil"

var (
	Trace *log.Logger
	Info  *log.Logger
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]
	log        []LogElement
	maxSeq     int
	done       []int
	offset     int
}

type LogElement struct {
	np        int
	na        int
	va        interface{}
	isDecided bool
}

type PrepareArgs struct {
	Seq int
	N   int
}

type PrepareReply struct {
	Np int
	Na int
	Va interface{}
}

type AcceptArgs struct {
	From    int
	Seq     int
	N       int
	V       interface{}
	Decided bool
	MaxDone int
}

type AcceptReply int

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func init() {
	log.SetOutput(ioutil.Discard)
	Trace = log.New(ioutil.Discard, "", log.Lmicroseconds)
	Info = log.New(ioutil.Discard, "", log.Lmicroseconds)
}

func maxInt(a, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

func (px *Paxos) prettyPrintLog(h string) {
	state := make([]rune, len(px.log))
	for i := 0; i < len(px.log); i++ {
		if px.log[i].isDecided {
			state[i] = '$'
		} else {
			state[i] = '.'
		}
	}

	Info.Printf("[%s] [%d] [%s] [%d] [%v]\n", h, px.me, string(state), px.offset, px.done)
}

func (px *Paxos) ensureLogElementExists(index int) bool {
	Trace.Printf("ensureLogElementExists<- [%d] [%d] [%d] [%d]\n", px.me, index, len(px.log), cap(px.log))

	px.maxSeq = maxInt(px.maxSeq, index+px.offset)

	if index < len(px.log) {
		return true
	}

	px.log = px.log[:cap(px.log)]

	if index < cap(px.log) {
		return true
	}

	px.log = append(px.log, LogElement{})

	return px.ensureLogElementExists(index)
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	Trace.Printf("Prepare<- [%d] [%d] [%d]\n", px.me, args.Seq, args.N)

	px.mu.Lock()
	defer px.mu.Unlock()

	if args.Seq < px.offset {
		return fmt.Errorf("[%s] Seq: [%d] already forgotten at paxos peer: [%d]", "Prepare", args.Seq, px.me)
	}

	args.Seq -= px.offset

	px.ensureLogElementExists(args.Seq)

	elem := &px.log[args.Seq]

	if args.N > elem.np {
		elem.np = args.N
	}

	reply.Np = elem.np
	reply.Na = elem.na
	reply.Va = elem.va

	Trace.Printf("<-Prepare [%d] [%d] [%d] [%v]\n", px.me, reply.Np, reply.Na, reply.Va)

	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	if args.Decided {
		Trace.Printf("Decided<- [%d] [%d] [%d]\n", px.me, args.Seq, args.N)
	} else {
		Trace.Printf("Accept<- [%d] [%d] [%d]\n", px.me, args.Seq, args.N)
	}

	go px.recdDone(args.From, args.MaxDone)

	px.mu.Lock()
	defer px.mu.Unlock()

	if args.Seq < px.offset {
		if args.Decided {
			Info.Printf("[%s] Seq: [%d] already forgotten at paxos peer: [%d]", "Decided", args.Seq, px.me)
			return nil
		} else {
			return fmt.Errorf("[%s] Seq: [%d] already forgotten at paxos peer: [%d]", "Accept", args.Seq, px.me)
		}
	}

	args.Seq -= px.offset

	px.ensureLogElementExists(args.Seq)

	elem := &px.log[args.Seq]

	if args.N >= elem.np {
		elem.np = args.N
		elem.na = args.N
		elem.va = args.V
		if args.Decided {
			elem.isDecided = args.Decided
			px.prettyPrintLog("decided")
		}
	}

	*reply = AcceptReply(elem.np)

	Trace.Printf("<-Accept [%d] [%d]\n", px.me, int(*reply))

	return nil
}

func (px *Paxos) recdDone(i int, seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	Trace.Printf("recdDone<- [%d] [%d] [%d] [%d] [%v]\n", px.me, i, seq, px.offset, px.done)

	px.done[i] = maxInt(px.done[i], seq)

	mm := px.done[0]
	for _, e := range px.done[1:] {
		if e < mm {
			mm = e
		}
	}

	if px.offset <= mm {
		px.log = px.log[mm-px.offset+1:]
		newLog := make([]LogElement, len(px.log))
		copy(newLog, px.log)
		px.log = newLog
		px.offset = mm + 1
		px.prettyPrintLog("done")
	}

	Trace.Printf("<-recdDone [%d] [%d] [%v]\n", px.me, px.offset, px.done)
}

func (px *Paxos) broadcastPrepare(seq, n int) (v interface{}, ok bool) {
	type PrepareChanResponse struct {
		ok    bool
		reply PrepareReply
	}

	prepareReplies := make(chan PrepareChanResponse)

	for i, p := range px.peers {
		go func(i int, p string) {
			var ok bool
			args := PrepareArgs{Seq: seq, N: n}
			var reply PrepareReply
			if i == px.me {
				if err := px.Prepare(&args, &reply); err == nil {
					ok = true
				} else {
					ok = false
				}
			} else {
				ok = call(p, "Paxos.Prepare", args, &reply)
			}
			prepareReplies <- PrepareChanResponse{ok, reply}
		}(i, p)
	}

	maxProposal := 0
	recdCount := 0

	for i := range px.peers {
		response := <-prepareReplies
		if response.ok {
			recdCount++
			reply := response.reply
			if reply.Na > maxProposal {
				maxProposal = reply.Na
				v = reply.Va
			}
			if recdCount > len(px.peers)/2 {
				go func(ch chan PrepareChanResponse, r int) {
					for i := 0; i < r; i++ {
						<-ch
					}
				}(prepareReplies, len(px.peers)-1-i)

				return v, true
			}
		}
	}

	return nil, false
}

func (px *Paxos) broadcastAccept(seq, n int, v interface{}) (maxProposal int, ok bool) {
	type AcceptChanResponse struct {
		ok    bool
		reply AcceptReply
	}

	acceptReplies := make(chan AcceptChanResponse)

	for i, p := range px.peers {
		go func(i int, p string) {
			var ok bool
			args := AcceptArgs{Seq: seq, N: n, V: v, MaxDone: px.done[px.me], From: px.me}
			var reply AcceptReply
			if i == px.me {
				if err := px.Accept(&args, &reply); err == nil {
					ok = true
				} else {
					ok = false
				}
			} else {
				ok = call(p, "Paxos.Accept", args, &reply)
			}
			acceptReplies <- AcceptChanResponse{ok, reply}
		}(i, p)
	}

	maxProposal = n
	acceptOkCount := 0

	for i := range px.peers {
		response := <-acceptReplies
		if response.ok {
			reply := response.reply
			if int(reply) == n {
				acceptOkCount++
			}
			maxProposal = maxInt(maxProposal, int(reply))
			if acceptOkCount > len(px.peers)/2 {
				go func(ch chan AcceptChanResponse, r int) {
					for i := 0; i < r; i++ {
						<-ch
					}
				}(acceptReplies, len(px.peers)-1-i)

				return n, true
			}
		}
	}

	return maxProposal, false
}

func (px *Paxos) broadcastDecided(seq, n int, v interface{}) {
	var mustAccept func(i int, p string)
	mustAccept = func(i int, p string) {
		if px.dead {
			Trace.Printf("broadcastDecided - dead - [%d] [%d] [%d]\n", px.me, seq, n)
			return
		}

		var ok bool
		args := AcceptArgs{Seq: seq, N: n, V: v, Decided: true, MaxDone: px.done[px.me], From: px.me}
		var reply AcceptReply
		if i == px.me {
			if err := px.Accept(&args, &reply); err == nil {
				ok = true
			} else {
				ok = false
			}
		} else {
			ok = call(p, "Paxos.Accept", args, &reply)
		}

		if !ok {
			time.AfterFunc(time.Millisecond*100, func() {
				mustAccept(i, p)
			})
		}
	}

	for i, p := range px.peers {
		go mustAccept(i, p)
	}
}

func (px *Paxos) propose(seq, maxProposal int, v interface{}) {
	Trace.Printf("propose [%d] [%d] [%d]\n", px.me, seq, maxProposal)

	if px.dead {
		Trace.Printf("propose - dead - [%d] [%d] [%d]\n", px.me, seq, maxProposal)
		return
	}

	px.mu.Lock()
	forgotten := seq < px.offset
	px.mu.Unlock()

	if forgotten {
		Info.Printf("propose - forgotten - [%d] [%d]\n", px.me, seq)
		return
	}

	n := (((maxProposal >> 3) + 1) << 3) + px.me

	newV, prepareOk := px.broadcastPrepare(seq, n)

	if !prepareOk {
		time.AfterFunc(time.Millisecond*100, func() {
			px.propose(seq, n, v)
		})
		return
	}

	if newV != nil {
		v = newV
	}

	newMaxProposal, acceptOk := px.broadcastAccept(seq, n, v)

	if !acceptOk {
		time.AfterFunc(time.Millisecond*100, func() {
			px.propose(seq, newMaxProposal, v)
		})
		return
	}

	px.broadcastDecided(seq, n, v)
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	Trace.Printf("Start [%d] [%d]\n", px.me, seq)

	px.mu.Lock()
	defer px.mu.Unlock()

	if seq < px.offset {
		return
	}

	px.ensureLogElementExists(seq - px.offset)

	elem := px.log[seq-px.offset]

	go func() {
		px.propose(seq, elem.np, v)
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	Trace.Printf("Done [%d] [%d]\n", px.me, seq)
	px.recdDone(px.me, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	Trace.Printf("Min [%d] [%d]\n", px.me, px.offset)
	return px.offset
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	Trace.Printf("Status [%d] [%d]\n", px.me, seq)

	px.mu.Lock()
	defer px.mu.Unlock()

	if seq >= px.offset && seq <= px.maxSeq && px.log[seq-px.offset].isDecided {
		return true, px.log[seq-px.offset].va
	}

	return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	px.log = make([]LogElement, 0)
	px.maxSeq = -1
	px.done = make([]int, len(px.peers))
	for i := range px.done {
		px.done[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
