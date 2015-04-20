package kvpaxos

import "hash/fnv"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
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

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
