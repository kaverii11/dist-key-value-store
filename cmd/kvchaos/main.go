// kvchaos is a correctness load test: workers write unique keys through
// the gRPC API for a fixed time while something else (scripts/chaos.sh)
// hard-kills the leader. Every write the cluster acknowledged is
// remembered; at the end it checks that
//  1. every acknowledged write can be read back from the leader, and
//  2. all nodes converge to the same commit index and key count.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kvv1 "github.com/kaverii11/dist-key-value-store/gen/kv/v1"
	"github.com/kaverii11/dist-key-value-store/internal/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type cluster struct {
	mu      sync.Mutex
	clients map[string]kvv1.KVClient // addr -> client
	addrs   []string
	leader  string
}

func newCluster(addrs []string) (*cluster, error) {
	c := &cluster{clients: map[string]kvv1.KVClient{}, addrs: addrs}
	for _, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.clients[a] = kvv1.NewKVClient(conn)
	}
	return c, nil
}

// alias maps an address a node advertises (e.g. node2:9002 inside Docker)
// to the address we dial (e.g. localhost:9002), by matching the port.
func (c *cluster) alias(advertised string) string {
	if _, ok := c.clients[advertised]; ok {
		return advertised
	}
	if i := strings.LastIndex(advertised, ":"); i >= 0 {
		port := advertised[i:]
		for _, a := range c.addrs {
			if strings.HasSuffix(a, port) {
				return a
			}
		}
	}
	return ""
}

func (c *cluster) currentLeader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

// findLeader asks every node for its status and picks the one that says
// it is leader.
func (c *cluster) findLeader() string {
	for _, a := range c.addrs {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		st, err := c.clients[a].Status(ctx, &kvv1.StatusRequest{})
		cancel()
		if err != nil {
			logErrOnce(a+" (status)", err)
		}
		if err == nil && st.State == "Leader" {
			c.mu.Lock()
			c.leader = a
			c.mu.Unlock()
			return a
		}
	}
	return ""
}

// setLeader records the leader's address; "" clears it, so the next
// request searches for the leader again.
func (c *cluster) setLeader(a string) {
	c.mu.Lock()
	c.leader = a
	c.mu.Unlock()
}

type stats struct {
	acked, notLeader, unavailable, otherErr atomic.Int64
	lastAck                                 atomic.Int64 // unix nanos of the latest successful write
	maxGap                                  atomic.Int64 // longest time with no successful write
}

func main() {
	addrsFlag := flag.String("addrs", "localhost:9001,localhost:9002,localhost:9003", "comma-separated gRPC addresses of all nodes")
	workers := flag.Int("workers", 16, "concurrent writers")
	dur := flag.Duration("duration", 40*time.Second, "how long to write")
	settle := flag.Duration("settle", 15*time.Second, "max time to wait for all nodes to converge after writing")
	flag.Parse()

	c, err := newCluster(strings.Split(*addrsFlag, ","))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for c.findLeader() == "" {
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("leader at start: %s; %d workers for %s\n", c.currentLeader(), *workers, *dur)

	var st stats
	st.lastAck.Store(time.Now().UnixNano())
	run := fmt.Sprintf("r%d", time.Now().Unix()%100000)
	ackedKeys := make([][]string, *workers)
	stop := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for seq := 0; time.Now().Before(stop); seq++ {
				key := fmt.Sprintf("%s-w%02d-%06d", run, w, seq)
				// Retry the same key until it is acknowledged (a put is
				// idempotent, so a retry after an unclear failure is safe).
				for time.Now().Before(stop) {
					leader := c.currentLeader()
					if leader == "" {
						if leader = c.findLeader(); leader == "" {
							time.Sleep(50 * time.Millisecond)
							continue
						}
					}
					var hdr metadata.MD
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					_, err := c.clients[leader].Put(ctx, &kvv1.PutRequest{Key: key, Value: "v-" + key}, grpc.Header(&hdr))
					cancel()
					if err == nil {
						st.acked.Add(1)
						now := time.Now().UnixNano()
						if gap := now - st.lastAck.Swap(now); gap > st.maxGap.Load() {
							st.maxGap.Store(gap)
						}
						ackedKeys[w] = append(ackedKeys[w], key)
						break
					}
					logErrOnce(leader, err)
					switch status.Code(err) {
					case codes.FailedPrecondition: // not leader: follow the hint, else search
						st.notLeader.Add(1)
						if h := hdr.Get(grpcapi.LeaderHeader); len(h) > 0 {
							c.setLeader(c.alias(h[0]))
						} else {
							c.setLeader("")
						}
					case codes.Unavailable, codes.DeadlineExceeded: // node down or commit timed out
						st.unavailable.Add(1)
						c.setLeader("")
						time.Sleep(50 * time.Millisecond)
					default:
						st.otherErr.Add(1)
						time.Sleep(50 * time.Millisecond)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	total := st.acked.Load()
	fmt.Printf("\nwrites acknowledged: %d (%.0f/s)\n", total, float64(total)/dur.Seconds())
	fmt.Printf("retries: not-leader %d, unavailable/timeout %d, other %d\n", st.notLeader.Load(), st.unavailable.Load(), st.otherErr.Load())
	fmt.Printf("longest gap with no acknowledged write (failover window): %s\n", time.Duration(st.maxGap.Load()).Round(time.Millisecond))

	// 1. Every acknowledged write must be readable from the leader.
	var all []string
	for _, ks := range ackedKeys {
		all = append(all, ks...)
	}
	missing, wrong, unreadable := 0, 0, 0
	for _, k := range all {
		for attempt := 0; ; attempt++ {
			if attempt == 100 {
				unreadable++
				break
			}
			leader := c.currentLeader()
			if leader == "" {
				leader = c.findLeader()
			}
			if leader == "" {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := c.clients[leader].Get(ctx, &kvv1.GetRequest{Key: k})
			cancel()
			if err == nil {
				if resp.Value != "v-"+k {
					wrong++
				}
				break
			}
			if status.Code(err) == codes.NotFound {
				missing++
				break
			}
			c.setLeader("")
			time.Sleep(50 * time.Millisecond)
		}
	}
	fmt.Printf("acknowledged writes lost: %d, wrong value: %d, unreadable: %d (checked %d)\n", missing, wrong, unreadable, len(all))

	// 2. All nodes should converge (same commit index and key count).
	deadline := time.Now().Add(*settle)
	var lines []string
	converged := false
	for time.Now().Before(deadline) {
		lines = lines[:0]
		commits, keys := map[uint64]bool{}, map[int64]bool{}
		up := 0
		for _, a := range c.addrs {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			s, err := c.clients[a].Status(ctx, &kvv1.StatusRequest{})
			cancel()
			if err != nil {
				lines = append(lines, fmt.Sprintf("  %s: down (%v)", a, status.Code(err)))
				continue
			}
			up++
			commits[s.CommitIndex], keys[s.Keys] = true, true
			lines = append(lines, fmt.Sprintf("  %s %-6s %-9s term=%d commit=%d lastLog=%d keys=%d", a, s.Id, s.State, s.Term, s.CommitIndex, s.LastLogIndex, s.Keys))
		}
		if up == len(c.addrs) && len(commits) == 1 && len(keys) == 1 {
			converged = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	sort.Strings(lines)
	fmt.Println("node status:")
	fmt.Println(strings.Join(lines, "\n"))
	if converged {
		fmt.Println("all nodes converged: YES")
	} else {
		fmt.Println("all nodes converged: NO")
	}
	if missing > 0 || wrong > 0 || unreadable > 0 || !converged {
		os.Exit(1)
	}
}

var seenErr sync.Map

// logErrOnce prints the first error of each (node, code) pair, with a
// timestamp, so a failover can be followed from the client's side.
func logErrOnce(addr string, err error) {
	k := addr + "|" + status.Code(err).String()
	if _, loaded := seenErr.LoadOrStore(k, true); !loaded {
		fmt.Printf("  %s first %s from %s: %s\n", time.Now().Format("15:04:05.000"), status.Code(err), addr, status.Convert(err).Message())
	}
}
