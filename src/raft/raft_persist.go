package raft

import (
	"bytes"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {

	w := &bytes.Buffer{}
	e := labgob.NewEncoder(w)

	e.Encode(rf.log.Len())
	for i := rf.log.startIndex; i <= rf.log.BackIndex(); i++ {
		e.Encode(rf.log.GetByIndex(i))
	}
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	rf.log = NewBuffer[*LogEntry]()

	// 加载日志
	nLog := 0
	if err := d.Decode(&nLog); err != nil {
		panic(err)
	}
	for i := 0; i < nLog; i++ {
		log := &LogEntry{}
		if err := d.Decode(log); err != nil {
			panic(err)
		}
		rf.log.Push(log)
	}

	// 加载其他元信息
	if err := d.Decode(&rf.currentTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.commitIndex); err != nil {
		panic(err)
	}

}
