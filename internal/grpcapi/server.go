// Package grpcapi serves the KV client API over gRPC (proto/kv/v1). It is a
// thin adapter over internal/api.Server, so the gRPC path and the original
// TCP path share the same leader-only rules.
package grpcapi

import (
	"context"
	"errors"

	kvv1 "github.com/kaverii11/dist-key-value-store/gen/kv/v1"
	"github.com/kaverii11/dist-key-value-store/internal/api"
	"github.com/kaverii11/dist-key-value-store/internal/raft"
	"github.com/kaverii11/dist-key-value-store/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// LeaderHeader is the response header a non-leader sets to the leader's
// gRPC address, so a client can retry there.
const LeaderHeader = "x-leader-addr"

type Server struct {
	kvv1.UnimplementedKVServer
	api       *api.Server
	node      *raft.Node
	fsm       *store.Store
	grpcAddrs map[string]string // node ID -> gRPC address
}

func New(apiSrv *api.Server, node *raft.Node, fsm *store.Store, grpcAddrs map[string]string) *Server {
	return &Server{api: apiSrv, node: node, fsm: fsm, grpcAddrs: grpcAddrs}
}

func (s *Server) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	if err := s.api.Put(req.GetKey(), req.GetValue()); err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &kvv1.PutResponse{}, nil
}

func (s *Server) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	v, found, err := s.api.Get(req.GetKey())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	if !found {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &kvv1.GetResponse{Value: v}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	if err := s.api.Delete(req.GetKey()); err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &kvv1.DeleteResponse{}, nil
}

func (s *Server) Status(ctx context.Context, _ *kvv1.StatusRequest) (*kvv1.StatusResponse, error) {
	return &kvv1.StatusResponse{
		Id:           s.node.ID(),
		State:        s.node.State().String(),
		Term:         s.node.CurrentTerm(),
		LeaderId:     s.node.LeaderID(),
		LeaderAddr:   s.node.LeaderAddr(s.grpcAddrs),
		CommitIndex:  s.node.CommitIndex(),
		LastLogIndex: s.node.LastLogIndex(),
		Keys:         int64(s.fsm.Len()),
	}, nil
}

// toStatus maps internal errors to gRPC codes. Not-leader is
// FAILED_PRECONDITION plus the leader's address in a response header;
// a proposal that didn't commit in time is UNAVAILABLE (safe to retry:
// the write may or may not have been applied, so clients should use
// idempotent writes, which a plain put is).
func (s *Server) toStatus(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		if addr := s.node.LeaderAddr(s.grpcAddrs); addr != "" {
			_ = grpc.SetHeader(ctx, metadata.Pairs(LeaderHeader, addr))
		}
		return status.Error(codes.FailedPrecondition, "not leader")
	case errors.Is(err, raft.ErrProposeTimeout):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
