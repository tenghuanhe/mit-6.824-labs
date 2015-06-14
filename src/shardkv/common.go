package shardkv

import "hash/fnv"
import "time"
import "encoding/gob"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotFound   = "ErrNotFound"
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

type GetShardArgs struct {
	Shard int
	Num   int
}

type GetShardReply struct {
	Err  Err
	Data ShardData
}

const PingInterval = time.Millisecond * 100

const DeadPings = 5

type PingArgs struct {
	Me   int
	Num  int
	Bid  int
	Done bool
}

type PingReply struct {
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func hashConfigBid(c *ConfigBid) int64 {
	h := fnv.New64a()
	encoder := gob.NewEncoder(h)
	encoder.Encode(c.Config.Num)
	encoder.Encode(c.Config.Shards)
	encoder.Encode(c.Bid)
	return int64(h.Sum64() >> 1)
}
