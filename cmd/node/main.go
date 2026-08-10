// cmd/node is the entrypoint for a cluster node. In its current form (M1)
// it just exposes the storage engine over a stdin REPL, so WAL durability
// and crash recovery can be demoed by killing and restarting the process.
// Config/transport/raft wiring is added in later milestones.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/kaverii11/dist-key-value-store/internal/store"
)

func main() {
	dataPath := flag.String("data", "data/node.wal", "path to this node's WAL file")
	flag.Parse()

	s, err := store.Open(*dataPath)
	if err != nil {
		log.Fatalf("failed to open store at %s: %v", *dataPath, err)
	}
	defer s.Close()

	fmt.Printf("node ready — %d key(s) recovered from %s\n", s.Len(), *dataPath)
	fmt.Println("commands: put <key> <value> | get <key> | del <key> | quit")

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

		case "quit", "exit":
			return

		default:
			fmt.Printf("unknown command %q\n", fields[0])
		}
	}
}
