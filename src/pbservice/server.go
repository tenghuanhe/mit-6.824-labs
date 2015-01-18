package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	mu         sync.RWMutex
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	curr       viewservice.View
	data       map[string]string
}

func (pb *PBServer) put(args *PutArgs, reply *PutReply) {
	if args.DoHash {
		prevValue := pb.data[args.Key]
		args.Value = strconv.Itoa(int(hash(prevValue + args.Value)))
		reply.PreviousValue = prevValue
	}

	pb.data[args.Key] = args.Value

	reply.Err = OK
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.curr.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.curr.Backup != "" {
		if ok := call(pb.curr.Backup, "PBServer.BPut", args, reply); !ok {
			return fmt.Errorf("Put [%s]: Error talking to backup [%s]", pb.curr.Primary, pb.curr.Backup)
		}

		if reply.Err != OK {
			return nil
		}
	}

	pb.put(args,reply)

	return nil
}

func (pb *PBServer) BPut(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.curr.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.put(args,reply)

	return nil
}

func (pb *PBServer) get(args *GetArgs, reply *GetReply) {
	if _, ok := pb.data[args.Key]; !ok {
		reply.Err = ErrNoKey
		return
	}

	reply.Err = OK
	reply.Value = pb.data[args.Key]
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	if pb.me != pb.curr.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.curr.Backup != "" {
		if ok := call(pb.curr.Backup, "PBServer.BGet", args, reply); !ok {
			return fmt.Errorf("Get [%s]: Error talking to backup [%s]", pb.curr.Primary, pb.curr.Backup)
		}

		if reply.Err != OK {
			return nil
		}
	}

	pb.get(args, reply)

	return nil
}

func (pb *PBServer) BGet(args *GetArgs, reply *GetReply) error {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	if pb.me != pb.curr.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.get(args, reply)

	return nil
}

func (pb *PBServer) BSync(args *SyncArgs, reply *SyncReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.data = args.Data

	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	view, err := pb.vs.Ping(pb.curr.Viewnum)

	if err != nil {
		return
	}

	if view.Primary == pb.me && view.Backup != "" && view.Backup != pb.curr.Backup {
		var reply SyncReply
		if ok := call(view.Backup, "PBServer.BSync", SyncArgs{Data: pb.data}, &reply); !ok {
			log.Printf("Sync [%s]: Error talking to backup [%s]", view.Primary, view.Backup)
			return
		}
	}

	pb.curr = view
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})

	pb.data = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
