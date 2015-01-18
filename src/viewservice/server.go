package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	curr            View
	hasPrimaryAcked bool
	lastSeen        map[string]time.Time
}

func (vs *ViewServer) changeView(n uint, p, b string) {
	vs.curr = View{n, p, b}
	vs.hasPrimaryAcked = false
}

func (vs *ViewServer) promoteBackup() bool {
	if !vs.hasPrimaryAcked {
		return false
	}

	if vs.curr.Backup == "" {
		return false
	}

	vs.changeView(vs.curr.Viewnum+1, vs.curr.Backup, "")

	return true
}

func (vs *ViewServer) removeBackup() bool {
	if !vs.hasPrimaryAcked {
		return false
	}

	vs.changeView(vs.curr.Viewnum+1, vs.curr.Primary, "")

	return true
}

func (vs *ViewServer) addServer(id string) bool {
	if !vs.hasPrimaryAcked {
		return false
	}

	if vs.curr.Primary == "" {
		vs.changeView(1, id, "")
		return true
	}

	if vs.curr.Backup == "" {
		vs.changeView(vs.curr.Viewnum+1, vs.curr.Primary, id)
		return true
	}

	return false
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	id, viewnum := args.Me, args.Viewnum

	switch id {
	case vs.curr.Primary:
		if viewnum == vs.curr.Viewnum {
			vs.hasPrimaryAcked = true
			vs.lastSeen[vs.curr.Primary] = time.Now()
		} else {
			vs.promoteBackup()
		}
	case vs.curr.Backup:
		if viewnum == vs.curr.Viewnum {
			vs.lastSeen[vs.curr.Backup] = time.Now()
		} else {
			vs.removeBackup()
		}
	default:
		vs.addServer(id)
	}

	reply.View = vs.curr

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.curr

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	for id, t := range vs.lastSeen {
		if time.Since(t) >= DeadPings*PingInterval {
			switch id {
			case vs.curr.Primary:
				vs.promoteBackup()
			case vs.curr.Backup:
				vs.removeBackup()
			}
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.hasPrimaryAcked = true
	vs.lastSeen = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
