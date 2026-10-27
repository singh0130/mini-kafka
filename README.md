mini-kafka/
├── cmd/
│ └── broker/
│ └── main.go # Entry point for our server
├── internal/
│ ├── storage/ # The append-only log engine (Segments, Offsets)
│ ├── network/ # TCP server, serialization, and client protocols
│ ├── cluster/ # Replication and leader/follower coordination
│ └── consumer/ # Consumer groups and partition rebalancing
├── go.mod
└── README.md
