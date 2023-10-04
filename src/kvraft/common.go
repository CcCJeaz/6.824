package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrNotCommand  = "ErrNotCommand"
)

type Err string

type CommandRequest struct {
	Key       string
	Value     string
	Kind      int
	ClientId  int64
	CommandId int
}

type CommandResponse struct {
	Err   Err
	Value string
}

const (
	PUT = iota
	APPEND
	GET
)
