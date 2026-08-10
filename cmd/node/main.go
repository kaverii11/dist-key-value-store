// cmd/node is the entrypoint for a cluster node: it wires the storage
// engine, TCP transport, and Raft state machine together, and exposes the
// store over a stdin REPL for manual demo interaction.
//
// Through M4, replication isn't wired yet (see internal/raft's package
// doc), so put/get/delete here are local to this node only; M5 adds
// leader-only writes with majority replication.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/kaverii11/dist-key-value-store/internal/config"
	"github.com/kaverii11/dist-key-value-store/internal/raft"
	"github.com/kaverii11/dist-key-value-store/internal/store"
	"github.com/kaverii11/dist-key-value-store/internal/transport"
)

func main() {
	configPath := flag.String("config", "scripts/cluster.json", "path to cluster config JSON")
	id := flag.String("id", "", "this node's ID (must match an entry in the cluster config)")
	dataDir := flag.String("data-dir", "data", "directory for this node's WAL and raft state files")
	flag.Parse()

	if *id == "" {
		log.Fatal("-id is required, e.g. -id node1")
	}

	cluster, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load cluster config: %v", err)
	}
	self, ok := cluster.Node(*id)
	if !ok {
		log.Fatalf("node id %q not found in %s", *id, *configPath)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("failed to create data dir %s: %v", *dataDir, err)
	}
	walPath := filepath.Join(*dataDir, *id+".wal")
	statePath := filepath.Join(*dataDir, *id+"-raftstate.json")

	s, err := store.Open(walPath)
	if err != nil {
		log.Fatalf("failed to open store at %s: %v", walPath, err)
	}
	defer s.Close()

	node, err := raft.NewNode(raft.Params{
		ID:         *id,
		PeerAddrs:  cluster.PeerClusterAddrs(*id),
		Transport:  transport.TCPTransport{},
		StateStore: raft.NewStateStore(statePath),
	})
	if err != nil {
		log.Fatalf("failed to create raft node: %v", err)
	}

	clusterSrv, err := transport.Listen(self.ClusterAddr, node.HandleMessage)
	if err != nil {
		log.Fatalf("failed to listen on cluster address %s: %v", self.ClusterAddr, err)
	}
	defer clusterSrv.Close()

	node.Start()
	defer node.Stop()

	fmt.Printf("node %s ready — cluster:%s client:%s — %d key(s) recovered from %s\n",
		*id, self.ClusterAddr, self.ClientAddr, s.Len(), walPath)
	fmt.Println("commands: put <key> <value> | get <key> | del <key> | status | quit")
	fmt.Println("(replication lands in M5 — writes here are local to this node only)")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}

		switch strings.ToLower(fields[0]) {
		case "put":
			if len(fields) < 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			key, value := fields[1], strings.Join(fields[2:], " ")
			if err := s.Put(key, value); err != nil {
				fmt.Printf("error: %v\n", err)
				continue
			}
			fmt.Println("OK")

		case "get":
			if len(fields) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			if v, ok := s.Get(fields[1]); ok {
				fmt.Println(v)
			} else {
				fmt.Println("(not found)")
			}

		case "del", "delete":
			if len(fields) != 2 {
				fmt.Println("usage: del <key>")
				continue
			}
			if err := s.Delete(fields[1]); err != nil {
				fmt.Printf("error: %v\n", err)
				continue
			}
			fmt.Println("OK")

		case "status":
			fmt.Printf("id=%s state=%s term=%d leader=%s\n", node.ID(), node.State(), node.CurrentTerm(), node.LeaderID())

		case "quit", "exit":
			return

		default:
			fmt.Printf("unknown command %q\n", fields[0])
		}
	}
}
