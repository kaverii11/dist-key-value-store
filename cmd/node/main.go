// cmd/node is the entrypoint for a cluster node: it wires the storage
// engine, TCP transport, Raft state machine, and client API together, and
// exposes the store over a stdin REPL for manual demo interaction.
//
// Writes and reads go through the same leader-only path a remote kvctl
// client would use (internal/api.Server) — see the project plan's Design
// Decisions for why reads are leader-only too in this project.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	kvv1 "github.com/kaverii11/dist-key-value-store/gen/kv/v1"
	"github.com/kaverii11/dist-key-value-store/internal/api"
	"github.com/kaverii11/dist-key-value-store/internal/config"
	"github.com/kaverii11/dist-key-value-store/internal/grpcapi"
	"github.com/kaverii11/dist-key-value-store/internal/metrics"
	"github.com/kaverii11/dist-key-value-store/internal/raft"
	"github.com/kaverii11/dist-key-value-store/internal/store"
	"github.com/kaverii11/dist-key-value-store/internal/transport"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "scripts/cluster.json", "path to cluster config JSON")
	id := flag.String("id", "", "this node's ID (must match an entry in the cluster config)")
	dataDir := flag.String("data-dir", "data", "directory for this node's WAL and raft state files")
	metricsAddr := flag.String("metrics-addr", "", "serve Prometheus /metrics and /healthz on this address, e.g. :2112 (off if empty)")
	noREPL := flag.Bool("no-repl", false, "run without the stdin REPL (for containers); stop on SIGINT/SIGTERM")
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

	fsm, records, err := store.OpenForRaft(walPath)
	if err != nil {
		log.Fatalf("failed to open store at %s: %v", walPath, err)
	}
	defer fsm.Close()

	node, err := raft.NewNode(raft.Params{
		ID:              *id,
		PeerAddrs:       cluster.PeerClusterAddrs(*id),
		Transport:       transport.TCPTransport{},
		StateStore:      raft.NewStateStore(statePath),
		Store:           fsm,
		ReplayedRecords: records,
	})
	if err != nil {
		log.Fatalf("failed to create raft node: %v", err)
	}

	clusterSrv, err := transport.Listen(listenAddr(self.ClusterAddr), node.HandleMessage)
	if err != nil {
		log.Fatalf("failed to listen on cluster address %s: %v", self.ClusterAddr, err)
	}
	defer clusterSrv.Close()

	apiSrv := api.NewServer(node, fsm, cluster.ClientAddrs())
	clientSrv, err := transport.Listen(listenAddr(self.ClientAddr), apiSrv.Handle)
	if err != nil {
		log.Fatalf("failed to listen on client address %s: %v", self.ClientAddr, err)
	}
	defer clientSrv.Close()

	// gRPC client API (optional: only if this node has a grpcAddr in the config).
	var mets *metrics.Metrics
	if *metricsAddr != "" {
		mets = metrics.New(node, fsm)
	}
	if self.GRPCAddr != "" {
		var opts []grpc.ServerOption
		if mets != nil {
			opts = append(opts, grpc.UnaryInterceptor(mets.UnaryInterceptor))
		}
		gs := grpc.NewServer(opts...)
		kvv1.RegisterKVServer(gs, grpcapi.New(apiSrv, node, fsm, cluster.GRPCAddrs()))
		lis, err := net.Listen("tcp", listenAddr(self.GRPCAddr))
		if err != nil {
			log.Fatalf("failed to listen on gRPC address %s: %v", self.GRPCAddr, err)
		}
		go func() {
			if err := gs.Serve(lis); err != nil {
				log.Printf("gRPC server stopped: %v", err)
			}
		}()
		defer gs.GracefulStop()
	}
	if mets != nil {
		mux := http.NewServeMux()
		mux.Handle("/metrics", mets.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
		go func() {
			if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
				log.Printf("metrics server stopped: %v", err)
			}
		}()
	}

	node.Start()
	defer node.Stop()

	if *noREPL {
		fmt.Printf("node %s ready (no REPL) - cluster:%s client:%s grpc:%s metrics:%s - %d key(s) recovered\n",
			*id, self.ClusterAddr, self.ClientAddr, self.GRPCAddr, *metricsAddr, fsm.Len())
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		return
	}

	fmt.Printf("node %s ready — cluster:%s client:%s — %d key(s) recovered from %s\n",
		*id, self.ClusterAddr, self.ClientAddr, fsm.Len(), walPath)
	fmt.Println("commands: put <key> <value> | get <key> | del <key> | status | quit")

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
			if err := apiSrv.Put(key, value); err != nil {
				printClusterErr(node, cluster, err)
				continue
			}
			fmt.Println("OK")

		case "get":
			if len(fields) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			v, found, err := apiSrv.Get(fields[1])
			if err != nil {
				printClusterErr(node, cluster, err)
				continue
			}
			if found {
				fmt.Println(v)
			} else {
				fmt.Println("(not found)")
			}

		case "del", "delete":
			if len(fields) != 2 {
				fmt.Println("usage: del <key>")
				continue
			}
			if err := apiSrv.Delete(fields[1]); err != nil {
				printClusterErr(node, cluster, err)
				continue
			}
			fmt.Println("OK")

		case "status":
			fmt.Printf("id=%s state=%s term=%d leader=%s commitIndex=%d\n",
				node.ID(), node.State(), node.CurrentTerm(), node.LeaderID(), node.CommitIndex())

		case "quit", "exit":
			return

		default:
			fmt.Printf("unknown command %q\n", fields[0])
		}
	}
}

func printClusterErr(node *raft.Node, cluster config.ClusterConfig, err error) {
	if errors.Is(err, raft.ErrNotLeader) {
		fmt.Printf("not leader (leader hint: %s)\n", node.LeaderAddr(cluster.ClientAddrs()))
		return
	}
	fmt.Printf("error: %v\n", err)
}

// listenAddr turns an advertised address like "node1:9001" into ":9001", so
// in Docker the node listens on all interfaces while peers dial it by name.
// Loopback addresses (the local demo) are kept as they are.
func listenAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || host == "127.0.0.1" || host == "localhost" {
		return addr
	}
	return ":" + port
}
