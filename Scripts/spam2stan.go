package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/stan.go"
)

const (
	ClusterName = "test-cluster"
	ClientName  = "spammer"
	Channel     = "test"
)

func main() {
	// Инициализация Nats
	conn := StanConn(ClusterName, ClientName)
	defer conn.Close()

	file, _ := os.Open("test_data.txt")
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		conn.Publish("test", scanner.Bytes())
	}
}

func StanConn(clusterID, clientID string) stan.Conn {
	timeout_opt := stan.ConnectWait(time.Duration(1) * time.Minute)
	sc, err := stan.Connect(clusterID, clientID, timeout_opt)

	if err != nil {
		log.Fatalf("[Error] NATS Streaming server not found: %v", err)
	}
	fmt.Println("[Info] Connected to Nats streaming")
	return sc
}
