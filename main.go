package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/dustinkirkland/golang-petname"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	gossip "github.com/j-mroz/gossip-cluster/proto/gossip/v1"
	"github.com/j-mroz/gossip-cluster/server/cluster"
)

func main() {
	var joinAddr, hostAddr string
	flag.StringVar(&joinAddr, "join", "", "cluster node to join")
	flag.StringVar(&hostAddr, "host", "", "this node")
	flag.Parse()

	if hostAddr == "" {
		fmt.Println("Usage: gossip-cluster -host=[<addr>]:<port> [-join=[<addr>]:<port>]")
		os.Exit(1)
	}

	rand.Seed(time.Now().UTC().UnixNano())

	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)

	listener, err := net.Listen("tcp", hostAddr)

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		os.Exit(1)
	}

	nodeName := petname.Generate(2, "-")
	nodePort := listener.Addr().(*net.TCPAddr).Port
	node := cluster.NewNode(nodeName, uint16(nodePort))

	log.Printf("started node %s at %s\n", nodeName, hostAddr)

	gossip.RegisterGossipServer(grpcServer, node.GossipServer)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		wg.Done()
	}()

	if joinAddr != "" {
		node.RequestJoin(joinAddr)
	}
	wg.Wait()

}
