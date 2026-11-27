// Package metrics exposes Prometheus metrics for one node: gRPC request
// counts and latency, Raft state (term, leader, log indexes) and WAL fsync
// latency. Served on /metrics; scraped by Prometheus and drawn in Grafana
// (see deploy/).
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/kaverii11/dist-key-value-store/internal/raft"
	"github.com/kaverii11/dist-key-value-store/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Metrics struct {
	reg      *prometheus.Registry
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	fsync    prometheus.Histogram
}

// New registers all metrics for node (and fsm) in a fresh registry.
func New(node *raft.Node, fsm *store.Store) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		reg: reg,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kv_grpc_requests_total", Help: "gRPC requests by method and status code.",
		}, []string{"method", "code"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kv_grpc_request_duration_seconds", Help: "gRPC request latency by method.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"method"}),
		fsync: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "kv_wal_fsync_duration_seconds", Help: "Time spent in fsync per WAL append.",
			Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1},
		}),
	}
	reg.MustRegister(m.requests, m.latency, m.fsync)

	gauge := func(name, help string, f func() float64) {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, f))
	}
	gauge("kv_raft_term", "Current Raft term.", func() float64 { return float64(node.CurrentTerm()) })
	gauge("kv_raft_is_leader", "1 if this node is the leader.", func() float64 {
		if node.IsLeader() {
			return 1
		}
		return 0
	})
	gauge("kv_raft_state", "0 = follower, 1 = candidate, 2 = leader.", func() float64 { return float64(node.State()) })
	gauge("kv_raft_commit_index", "Highest committed log index.", func() float64 { return float64(node.CommitIndex()) })
	gauge("kv_raft_last_log_index", "Index of the last log entry.", func() float64 { return float64(node.LastLogIndex()) })
	gauge("kv_store_keys", "Keys in the applied state.", func() float64 { return float64(fsm.Len()) })

	store.OnFsync = func(d time.Duration) { m.fsync.Observe(d.Seconds()) }
	return m
}

// UnaryInterceptor records count and latency for every gRPC call.
func (m *Metrics) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	m.latency.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
	m.requests.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
	return resp, err
}

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
