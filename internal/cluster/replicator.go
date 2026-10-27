package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"mini-kafka/internal/storage"
	"net"
	"time"
)

type Replicator struct {
	LeaderAddr  string
	Topic       string
	PartitionID uint32
	LocalLog    *storage.Log
}

func NewReplicator(leaderAddr string, topic string, partitionID uint32, localLog *storage.Log) *Replicator {
	return &Replicator{
		LeaderAddr:  leaderAddr,
		Topic:       topic,
		PartitionID: partitionID,
		LocalLog:    localLog,
	}
}

// StartSync runs a background loop that continously pulls new records from the leader
func (r *Replicator) StartSync() {
	fmt.Printf("Starting replication worker for topic %s partition %d from leader %s... \n", r.Topic, r.PartitionID, r.LeaderAddr)

	for {
		// 1. Determine what offset the follower should fetch next.
		// In a simple log, the next offset to fetch is equal to the number of records we already have.
		// (We can check this by seeing what our next expected append offset is).

		// Let's open a connection to the leader broker
		conn, err := net.Dial("tcp", r.LeaderAddr)
		if err != nil {
			fmt.Printf("[Replicator] Failed to connect to leader: %v. Retrying in 2s...\n", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// Calculate current local offset we need to fetch
		fetchOffset := r.getNextFetchOffset()

		// 2. Build the FETCH Request (using our exact Milestone 2 wire protocol)
		// Layout: [Cmd: (1: CmdFetch)] [TopicLen (2)] [Topic (N)] [Partition (4)] [Offset (8)]
		fetchReqLen := 1 + 2 + len(r.Topic) + 4 + 8
		fetchReq := make([]byte, fetchReqLen)

		fetchReq[0] = 2 //CmdFetch
		binary.BigEndian.PutUint16(fetchReq[1:3], uint16(len(r.Topic)))
		copy(fetchReq[3:3+len(r.Topic)], r.Topic)

		pIdx := 3 + len(r.Topic)
		binary.BigEndian.PutUint32(fetchReq[pIdx:pIdx+4], r.PartitionID)

		offIdx := pIdx + 4
		binary.BigEndian.PutUint64(fetchReq[offIdx:offIdx+8], fetchOffset)

		// Send fetch request to leader
		if _, err := conn.Write(fetchReq); err != nil {
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// 3. Read response from Leader
		// Response layout: [Status (1 byte)] [ValueLen (4 bytes)] [Value (N bytes)]
		satusBuf := make([]byte, 1)
		if _, err = io.ReadFull(conn, satusBuf); err != nil {
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		if satusBuf[0] != 0 {
			// Offset not found yet (leader doesn't have new data), wait and poll again
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			conn.Close()
			continue
		}

		valLen := binary.BigEndian.Uint32(lenBuf)

		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(conn, valBuf); err != nil {
			conn.Close()
			continue
		}

		conn.Close()

		// 4. Append the replicated record to the follower's local storage log!
		assignedOffset, err := r.LocalLog.Append(valBuf)
		if err != nil {
			fmt.Printf("[Replicator Error] Failed to write replicated record: %v\n", err)
			continue
		}

		fmt.Printf("[Follower] Replicated new record from leader at offset %d: %s\n", assignedOffset, string(valBuf))
	}
}

// Helper to determine the next offset to fetch optimally in O(1) time
func (r *Replicator) getNextFetchOffset() uint64 {
	// Because our local log's nextOffset equals the total number of records
	// We've successfully stored, it is precisely the offset we need to fetch next!
	return r.LocalLog.NextOffset()
}
