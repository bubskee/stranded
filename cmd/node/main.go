package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bubskee/stranded/node"
	raftpb "github.com/bubskee/stranded/proto/raftpb/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	id := flag.String(
		"id",
		"",
		"node ID",
	)
	listenAddr := flag.String(
		"listen",
		"",
		"gRPC listen address",
	)
	peersRaw := flag.String(
		"peers",
		"",
		"comma-separated peerID=address pairs",
	)
	dataDir := flag.String(
		"data-dir",
		"",
		"persistent state directory",
	)

	flag.Parse()

	if *id == "" {
		return errors.New("--id is required")
	}
	if *listenAddr == "" {
		return errors.New("--listen is required")
	}
	if *dataDir == "" {
		return errors.New("--data-dir is required")
	}

	peers, err := parsePeers(*peersRaw)
	if err != nil {
		return err
	}

	nodeID := node.PeerID(*id)

	if _, ok := peers[nodeID]; ok {
		return fmt.Errorf(
			"peer list contains local node %q",
			nodeID,
		)
	}

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listenAddr, err)
	}
	defer listener.Close()

	clients := make(
		map[node.PeerID]raftpb.RaftServiceClient,
		len(peers),
	)

	var conns []*grpc.ClientConn

	for peerID, addr := range peers {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)
		if err != nil {
			return fmt.Errorf(
				"dial peer %s at %s: %w",
				peerID,
				addr,
				err,
			)
		}

		conns = append(conns, conn)
		clients[peerID] = raftpb.NewRaftServiceClient(conn)
	}

	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()

	transport := node.NewGRPCTransport(clients)

	n, err := node.NewWithTransport(
		node.Config{
			ID:      nodeID,
			Peers:   peers,
			DataDir: *dataDir,

			TickInterval:       10 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
		},
		transport,
	)
	if err != nil {
		return fmt.Errorf("create node: %w", err)
	}

	server := grpc.NewServer()

	raftpb.RegisterRaftServiceServer(
		server,
		node.NewGRPCServer(n),
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	go func() {
		for {
			select {
			case entry := <-n.ApplyCh():
				if entry.Command == nil {
					log.Printf(
						"applied no-op: term=%d index=%d",
						entry.Term,
						entry.Index,
					)
					continue
				}

				log.Printf(
					"applied command: term=%d index=%d command=%q",
					entry.Term,
					entry.Index,
					entry.Command,
				)

			case <-ctx.Done():
				return
			}
		}
	}()

	nodeErr := make(chan error, 1)
	go func() {
		nodeErr <- n.Run(ctx)
	}()

	log.Printf(
		"node %s listening on %s with %d peers",
		nodeID,
		*listenAddr,
		len(peers),
	)

	select {
	case <-ctx.Done():
		server.GracefulStop()
		return nil

	case err := <-serveErr:
		stop()

		if err != nil {
			return fmt.Errorf("gRPC server: %w", err)
		}

		return nil

	case err := <-nodeErr:
		server.GracefulStop()

		if errors.Is(err, context.Canceled) {
			return nil
		}

		return fmt.Errorf("run node: %w", err)
	}
}

func parsePeers(raw string) (map[node.PeerID]string, error) {
	peers := make(map[node.PeerID]string)

	if strings.TrimSpace(raw) == "" {
		return peers, nil
	}

	for _, part := range strings.Split(raw, ",") {
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf(
				"invalid peer %q: want id=address",
				part,
			)
		}

		id = strings.TrimSpace(id)
		addr = strings.TrimSpace(addr)

		if id == "" {
			return nil, fmt.Errorf(
				"invalid peer %q: empty ID",
				part,
			)
		}

		if addr == "" {
			return nil, fmt.Errorf(
				"invalid peer %q: empty address",
				part,
			)
		}

		peerID := node.PeerID(id)

		if _, exists := peers[peerID]; exists {
			return nil, fmt.Errorf(
				"duplicate peer %q",
				peerID,
			)
		}

		peers[peerID] = addr
	}

	return peers, nil
}
