// kvctl is a minimal CLI client for the cluster's client API — mainly so
// the demo has something to type instead of raw TCP frames.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/kaverii11/dist-key-value-store/internal/api"
	"github.com/kaverii11/dist-key-value-store/internal/transport"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8001", "client address of a node to talk to")
	timeout := flag.Duration("timeout", 3*time.Second, "RPC timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	switch args[0] {
	case "put":
		if len(args) != 3 {
			usage()
		}
		resp := call(*addr, transport.MsgClientPutReq, api.ClientPutReq{Key: args[1], Value: args[2]}, *timeout)
		if !resp.OK {
			failWith(resp)
		}
		fmt.Println("OK")

	case "get":
		if len(args) != 2 {
			usage()
		}
		resp := call(*addr, transport.MsgClientGetReq, api.ClientGetReq{Key: args[1]}, *timeout)
		if !resp.OK {
			failWith(resp)
		}
		fmt.Println(resp.Value)

	case "del", "delete":
		if len(args) != 2 {
			usage()
		}
		resp := call(*addr, transport.MsgClientDeleteReq, api.ClientDeleteReq{Key: args[1]}, *timeout)
		if !resp.OK {
			failWith(resp)
		}
		fmt.Println("OK")

	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kvctl -addr host:port put <key> <value> | get <key> | del <key>")
	os.Exit(2)
}

func call(addr, msgType string, req any, timeout time.Duration) api.ClientResp {
	payload, err := json.Marshal(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	resp, err := transport.Call(addr, transport.Message{Type: msgType, Payload: payload}, timeout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	var cr api.ClientResp
	if err := json.Unmarshal(resp.Payload, &cr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	return cr
}

func failWith(r api.ClientResp) {
	if r.LeaderHint != "" {
		fmt.Fprintf(os.Stderr, "error: %s (leader is at %s)\n", r.Error, r.LeaderHint)
	} else {
		fmt.Fprintf(os.Stderr, "error: %s\n", r.Error)
	}
	os.Exit(1)
}
