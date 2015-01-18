package pbservice

import "hash/fnv"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Xid    int64
	Key    string
	Value  string
	DoHash bool // For PutHash
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Xid int64
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type SyncArgs struct {
	Data    map[string]string
	Replies map[int64]string
}

type SyncReply struct{}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
