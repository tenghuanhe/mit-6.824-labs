package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type serverInfo struct {
	lastPing  time.Time
	viewAcked uint
}

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	curr          View
	canUpdateView bool
	servers       map[string]serverInfo
}

func (vs *ViewServer) checkPrimary() {
	log.Printf("checkPrimary %v %t", vs.curr, vs.canUpdateView)
	if !vs.canUpdateView {
		return
	}

	p, b := vs.curr.Backup, ""

	// select new backup
	for id := range vs.servers {
		if id != p {
			b = id
			break
		}
	}

	vs.curr = View{vs.curr.Viewnum + 1, p, b}
	vs.canUpdateView = false
	log.Printf("END checkPrimary %v %t", vs.curr, vs.canUpdateView)
}

func (vs *ViewServer) checkBackup() {
	log.Printf("checkBackup %v %t", vs.curr, vs.canUpdateView)
	if !vs.canUpdateView {
		return
	}

	b := vs.curr.Backup

	if b == "" {
		p := vs.curr.Primary
		for id := range vs.servers {
			if id != p {
				b = id
				break
			}
		}
		if b != "" {
			vs.curr.Viewnum++
			vs.curr.Backup = b
			vs.canUpdateView = false
		}
		log.Printf("END 1 checkBackup %v %t", vs.curr, vs.canUpdateView)
		return
	}

	if _, ok := vs.servers[b]; !ok {
		p, b := vs.curr.Primary, ""
		for id := range vs.servers {
			if id != p {
				b = id
				break
			}
		}
		vs.curr.Viewnum++
		vs.curr.Backup = b
		vs.canUpdateView = false
	}
	log.Printf("END 2 checkBackup %v %t", vs.curr, vs.canUpdateView)
}

func (vs *ViewServer) addServer(id string) {
	log.Printf("addServer %v %t", vs.curr, vs.canUpdateView)
	if vs.curr.Primary == "" {
		vs.curr = View{1, id, ""}
		vs.canUpdateView = false
		log.Printf("END addServer 1 %v %t", vs.curr, vs.canUpdateView)
		return
	}

	if vs.curr.Backup == "" && vs.canUpdateView {
		vs.curr.Viewnum++
		vs.curr.Backup = id
		vs.canUpdateView = false
	}
	log.Printf("END addServer 2 %v %t", vs.curr, vs.canUpdateView)
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	id, viewnum := args.Me, args.Viewnum

	log.Printf("Ping %s %d", id, viewnum)

	vs.servers[id] = serverInfo{time.Now(), viewnum}

	if id == vs.curr.Primary && viewnum == vs.curr.Viewnum {
		vs.canUpdateView = true
		vs.checkBackup()
	}

	if viewnum == 0 {
		switch id {
		case vs.curr.Primary:
			vs.checkPrimary()
		case vs.curr.Backup:
			vs.checkBackup()
		default:
			vs.addServer(id)
		}
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

	log.Printf("TICK!")
	dead := make([]string, 0)

	for id, info := range vs.servers {
		if time.Since(info.lastPing) >= DeadPings*PingInterval {
			dead = append(dead, id)
		}
	}

	for _, id := range dead {
		delete(vs.servers, id)
	}

	if vs.curr.Primary != "" {
		if _, ok := vs.servers[vs.curr.Primary]; !ok {
			vs.checkPrimary()
		}
	}

	if vs.curr.Backup != "" {
		if _, ok := vs.servers[vs.curr.Backup]; !ok {
			vs.checkBackup()
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
	vs.canUpdateView = true
	vs.servers = make(map[string]serverInfo)

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
