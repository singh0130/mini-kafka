package network

import (
	"encoding/binary"
	"fmt"
	"io"
	"mini-kafka/internal/storage"
	"net"
	"sync"
)

// Request tyoes
const (
	CmdProduce = 1
	CmdFetch   = 2
	CmdPing    = 3
)

type Server struct {
	addr       string
	listener   net.Listener
	mu         sync.Mutex
	partitions map[string]*storage.Log // Key: "topic-partitionId"
	isClosed   bool
}

func NewServer(addr string) *Server {
	return &Server{
		addr:       addr,
		partitions: make(map[string]*storage.Log),
	}
}

// RegisterPartition attaches a storage log to a topic/partition path
func (s *Server) RegisterPartition(topic string, partitionID int, log *storage.Log) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partitionID)
	s.partitions[key] = log
}

func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	fmt.Printf("Broker listening on %s...\n", s.addr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			break
		}
		go s.handleConnection(conn)
	}
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Read command type (1 byte)
		cmdHeader := make([]byte, 1)
		_, err := io.ReadFull(conn, cmdHeader)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Connection read error: %v\n", err)
			}
			break
		}

		cmd := cmdHeader[0]

		switch cmd {
		case CmdProduce:
			s.handleProduce(conn)
		case CmdFetch:
			s.handleFetch(conn)
		case CmdPing:
			s.handlePing(conn)
		default:
			fmt.Printf("Unknown command recieved: %d\n", cmd)
			return
		}
	}
}

// Protocol Format for PRODUCE:
// [Cmd: 1 byte] [TopicLen: 2 bytes] [Topic: N bytes] [Partition: 4 bytes] [ValueLen: 4 bytes] [Value: N bytes]
// Response: [Error/Success: 1 byte] [Offset: 8 bytes]
func (s *Server) handleProduce(conn net.Conn) {
	// 1. Read Topic Length (2 bytes)
	topicLenBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, topicLenBuf); err != nil {
		return
	}
	topicLen := binary.BigEndian.Uint16(topicLenBuf)

	// 2. Read the Topic Name bytes (TopicLen bytes)
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(conn, topicBytes); err != nil {
		return
	}
	topic := string(topicBytes)

	// 3. Read PartitionID (4 bytes)
	partBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, partBuf); err != nil {
		return
	}
	partitionID := binary.BigEndian.Uint32(partBuf)

	// 4. Read Value Length (4 bytes)
	valLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, valLenBuf); err != nil {
		return
	}
	valLen := binary.BigEndian.Uint32(valLenBuf)

	// 5. Read the message Value bytes (ValLen bytes)
	valBytes := make([]byte, valLen)
	if _, err := io.ReadFull(conn, valBytes); err != nil {
		return
	}

	s.mu.Lock()
	key := fmt.Sprintf("%s-%d", topic, partitionID)
	logStore, exists := s.partitions[key]
	s.mu.Unlock()

	if !exists {
		conn.Write([]byte{1}) // Error status
		return
	}

	offset, err := logStore.Append(valBytes)
	if err != nil {
		conn.Write([]byte{1})
		return
	}

	// Send back success (0) and assigned offset (8 bytes)
	resp := make([]byte, 9)
	resp[0] = 0
	binary.BigEndian.PutUint64(resp[1:9], offset)
	conn.Write(resp)
}

// Protocol Format for FETCH:
// [Cmd: 1 byte] [TopicLen: 2 bytes] [Topic: N bytes] [Partition: 4 bytes] [Offset: 8 bytes]
// Response: [Status: 1 byte] [ValueLen: 4 bytes] [Value: N bytes]
func (s *Server) handleFetch(conn net.Conn) {
	// 1. Read Topic Length (2 bytes)
	topicLenBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, topicLenBuf); err != nil {
		return
	}
	topicLen := binary.BigEndian.Uint16(topicLenBuf)

	// 2. Read Topic Name bytes (TopicLen bytes)
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(conn, topicBytes); err != nil {
		return
	}
	topic := string(topicBytes)

	// 3. Read PartitionID (4 bytes)
	partBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, partBuf); err != nil {
		return
	}
	partitionID := binary.BigEndian.Uint32(partBuf)

	// 4. Read Offset (8 bytes)
	offBuf := make([]byte, 8)
	if _, err := io.ReadFull(conn, offBuf); err != nil {
		return
	}
	offset := binary.BigEndian.Uint64(offBuf)

	s.mu.Lock()
	key := fmt.Sprintf("%s-%d", topic, partitionID)
	logStore, exists := s.partitions[key]
	s.mu.Unlock()

	if !exists {
		conn.Write([]byte{1}) // Error status
		return
	}

	val, err := logStore.Read(offset)
	if err != nil {
		conn.Write([]byte{1}) // Error/Not found
		return
	}

	// Send response: [Status 0 (1 byte)] [ValueLen (4 bytes)] [Value (N bytes)]
	resp := make([]byte, 5+len(val))
	resp[0] = 0
	binary.BigEndian.PutUint32(resp[1:5], uint32(len(val)))
	copy(resp[5:], val)
	conn.Write(resp)
}

// Protocol Format for PING:
// [Cmd: 1 byte]
// Response: [Status: 1 byte (0 for OK)]
func (s *Server) handlePing(conn net.Conn) {
	// PING takes no extra payload. Just reply with a success status byte.
	conn.Write([]byte{0})
}

func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
}
