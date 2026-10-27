package main

import (
	"flag"
	"fmt"
	"log"
	"mini-kafka/internal/network"
	"mini-kafka/internal/storage"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 1. Parse Command-Line Configuration Flags
	portFlag := flag.String("port", "9092", "TCP port for the broker to listen on")
	dataDir := flag.String("data", "./data/partition-0", "Filesystem directory path for the partition log")
	topicFlag := flag.String("topic", "defaultTopicName", "Initial topic name hosted by this broker")
	partitionFlag := flag.Int("partition", 0, "Initial partition ID hosted by this broker")

	fmt.Println("========================================")
	fmt.Println("     STARTING MINI-KAFKA BROKER       ")
	fmt.Println("========================================")
	fmt.Printf(" -> TCP Port:      %s\n", *portFlag)
	fmt.Printf(" -> Storage Path:  %s\n", *dataDir)
	fmt.Printf(" -> Hosting:       Topic '%s' [Partition %d]\n", *topicFlag, *partitionFlag)
	fmt.Println("----------------------------------------")

	// 2. Initialize Storage Engine (Append-Only Log)
	partitionLog, err := storage.NewLog(*dataDir)
	if err != nil {
		log.Fatalf("[Critical] Failed to initialize storage log at %s: %v", *dataDir, err)
	}
	// Ensure disk segment files and indices are safely flushed and closed on exit
	defer partitionLog.Close()

	// 3. Initialize and confifure TCP Network Server
	addr := fmt.Sprintf(":%s", *portFlag)
	server := network.NewServer(addr)

	// Register the partition log with the TCP server's routing table
	server.RegisterPartition(*topicFlag, *partitionFlag, partitionLog)

	// 4. Setup Graceful Shutdown Handler (Signals)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Setup the TCP server listening loop in a background goroutine
	go func() {
		fmt.Printf("[Broker] Listening for client connections on %s...\n", addr)
		if err := server.Start(); err != nil {
			log.Printf("[Server Error] TCP listener stopped: %v\n", err)
		}
	}()

	// Block main goroutine until an interruption signal (Ctrl+C) is caught
	sig := <-sigChan
	fmt.Printf("\n[Signal Received] Caught %v. Initiating graceful shutdown...\n", sig)

	// Close network listener to immediately stop accepting new socket connections
	server.Close()

	// Deferred calls ('partitionLog.Close()') execute here, ensuring zero data loss
	fmt.Println("[Shutdown Complete] Broker stopped cleanly. All files saved to disk.")
}
