// kvctl — CLI for the DistributedKVStore cluster.
//
// Usage:
//
//	kvctl get  <key>        [--addr=...] [--consistency=strong|eventual]
//	kvctl put  <key> <val>  [--addr=...]
//	kvctl del  <key>        [--addr=...]
//	kvctl health            [--addr=...] [--peers=addr1,addr2,...]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/saitejasrivilli/DistributedKVStore/client/kvstore"
)

const defaultAddr = "http://localhost:8080"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "get":
		runGet(os.Args[2:])
	case "put":
		runPut(os.Args[2:])
	case "del", "delete":
		runDel(os.Args[2:])
	case "health":
		runHealth(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `kvctl — DistributedKVStore CLI

Commands:
  get  <key>        [--addr=http://host:port] [--consistency=eventual|strong]
  put  <key> <val>  [--addr=http://host:port]
  del  <key>        [--addr=http://host:port]
  health            [--addr=http://host:port] [--peers=addr1,addr2,...]

Defaults: --addr=http://localhost:8080  --consistency=eventual`)
}

func runGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	addr := fs.String("addr", defaultAddr, "node address")
	consistency := fs.String("consistency", "eventual", "eventual|strong")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl get <key>")
		os.Exit(1)
	}
	key := fs.Arg(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := kvstore.New(*addr).Get(ctx, key, *consistency)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("value:   %s\nversion: %d\n", r.Value, r.Version)
}

func runPut(args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	addr := fs.String("addr", defaultAddr, "node address")
	fs.Parse(args)

	if fs.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "usage: kvctl put <key> <value>")
		os.Exit(1)
	}
	key, value := fs.Arg(0), fs.Arg(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := kvstore.New(*addr).Put(ctx, key, value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("ok: true  version: %d  acks: %d\n", r.Version, r.Acks)
}

func runDel(args []string) {
	fs := flag.NewFlagSet("del", flag.ExitOnError)
	addr := fs.String("addr", defaultAddr, "node address")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: kvctl del <key>")
		os.Exit(1)
	}
	key := fs.Arg(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := kvstore.New(*addr).Delete(ctx, key); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("deleted")
}

func runHealth(args []string) {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	addr := fs.String("addr", defaultAddr, "primary node address")
	peers := fs.String("peers", "", "comma-separated peer addresses to probe concurrently")
	fs.Parse(args)

	addrs := []string{*addr}
	if *peers != "" {
		for _, p := range strings.Split(*peers, ",") {
			if p = strings.TrimSpace(p); p != "" {
				addrs = append(addrs, p)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := kvstore.HealthAll(ctx, addrs)

	allHealthy := true
	for _, n := range results {
		if n.Err != nil {
			allHealthy = false
			if n.Result != nil {
				fmt.Printf("[DOWN]    %s  role=%-8s  wal_size=%d  node_id=%s\n",
					n.Addr, n.Result.Role, n.Result.WALSize, n.Result.NodeID)
			} else {
				fmt.Printf("[UNREACHABLE] %s  err=%v\n", n.Addr, n.Err)
			}
			continue
		}
		r := n.Result
		quorum := "quorum=yes"
		if !r.QuorumAvailable {
			quorum = "quorum=NO"
		}
		fmt.Printf("[healthy] %s  role=%-8s  wal_size=%d  %s  node_id=%s\n",
			n.Addr, r.Role, r.WALSize, quorum, r.NodeID)
	}

	if !allHealthy {
		os.Exit(1)
	}
}
