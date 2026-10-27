package minikafka

import (
	"encoding/binary"
	"fmt"
	"io"
	"mini-kafka/internal/cluster"
	"mini-kafka/internal/consumer"
	"mini-kafka/internal/network"
	"mini-kafka/internal/storage"
	"net"
	"testing"
	"time"
)

func TestE2E_LogBrokerPipeline(t *testing.T) {
	// 1. Use Go's built-in temp directory management (clean cleanup)
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	// 2. Initialize Leader Storage & TCP Server
	leaderLog, err := storage.NewLog(leaderDir)
	if err != nil {
		t.Fatalf("Failed to create leader log: %v", err)
	}
	defer leaderLog.Close()

	leaderServer := network.NewServer("127.0.0.1:9092")
	leaderServer.RegisterPartition("analytics", 0, leaderLog)

	go func() {
		if err := leaderServer.Start(); err != nil {
			// Expected when server.Close() is called
			return
		}
	}()
	defer leaderServer.Close()
	time.Sleep(200 * time.Millisecond) // Give listener a moment to bind

	// 3. Initialize Follower Storage & Replication Worker
	followerLog, err := storage.NewLog(followerDir)
	if err != nil {
		t.Fatalf("Failed to create follower log: %v", err)
	}
	defer followerLog.Close()

	replicator := cluster.NewReplicator("127.0.0.1:9092", "analytics", 0, followerLog)
	go replicator.StartSync()
	time.Sleep(200 * time.Millisecond)

	// 4. Test Produce over TCP
	conn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		t.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	topic := "analytics"
	msg := []byte("test_event")

	// Pack PRODUCE request: [Cmd(1)] [TopicLen(2)] [Topic] [Partition(4)] [ValueLen(4)] [Value]
	reqLen := 1 + 2 + len(topic) + 4 + 4 + len(msg)
	req := make([]byte, reqLen)
	req[0] = 1 // CmdProduce
	binary.BigEndian.PutUint16(req[1:3], uint16(len(topic)))
	copy(req[3:3+len(topic)], topic)

	pIdx := 3 + len(topic)
	binary.BigEndian.PutUint32(req[pIdx:pIdx+4], 0)

	vLenIdx := pIdx + 4
	binary.BigEndian.PutUint32(req[vLenIdx:vLenIdx+4], uint32(len(msg)))

	valIdx := vLenIdx + 4
	copy(req[valIdx:], msg)

	if _, err := conn.Write(req); err != nil {
		t.Fatalf("Failed to write produce request: %v", err)
	}

	resp := make([]byte, 9)
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatalf("Failed to read produce response: %v", err)
	}

	if resp[0] != 0 {
		t.Fatalf("Broker returned error status on produce")
	}
	assignedOffset := binary.BigEndian.Uint64(resp[1:9])
	if assignedOffset != 0 {
		t.Errorf("Expected assigned offset 0, got %d", assignedOffset)
	}

	// 5. Verify Replication
	// Give the background replicator loop a brief moment to pull from the leader
	time.Sleep(2000 * time.Millisecond)
	syncedVal, err := followerLog.Read(0)
	if err != nil {
		t.Fatalf("Follower failed to replicate record: %v", err)
	}
	if string(syncedVal) != string(msg) {
		t.Errorf("Replicated content mismatch: expected '%s', got '%s'", string(msg), string(syncedVal))
	}

	// 6. Test Consumer Group Rebalancing
	coordinator := consumer.NewCoordinator()
	partitions := []int{0, 1}

	assigned1 := coordinator.JoinGroup("analytics-group", "analytics", "worker-1", partitions)
	if len(assigned1) != 2 {
		t.Errorf("Expected worker-1 to get all 2 partitions when alone, got %v", assigned1)
	}

	assigned2 := coordinator.JoinGroup("analytics-group", "analytics", "worker-2", partitions)
	worker1Updated := coordinator.JoinGroup("analytics-group", "analytics", "worker-1", partitions)

	totalAssigned := len(worker1Updated) + len(assigned2)
	if totalAssigned != 2 {
		t.Errorf("Partition loss detected during rebalance. Total assigned across workers: %d", totalAssigned)
	}

	// 7. Test watchdog timeout & dead consumer eviction
	// Refresh worker-1's lease
	coordinator.Heartbeat("analytics-group", "worker-1")

	time.Sleep(7000 * time.Millisecond)

	finalAssignment := coordinator.JoinGroup("analytics-group", "analytics", "worker-1", partitions)

	if len(finalAssignment) != 2 {
		t.Errorf("Watchdog failed to evict dead worker-2. Expected worker-1 to reclaim all 2 partitions, got %v", finalAssignment)
	}
	fmt.Println("E2E Test PASSED")
}
