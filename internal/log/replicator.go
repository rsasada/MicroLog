package log

import (
	"context"
	"sync"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	api "github.com/risasada/WriteALog/api/v1"
)

type Replicator struct {
	DialOptions	[]grpc.DialOption
	LocalServer	api.LogClient

	logger		*zap.Logger

	mu			sync.Mutex
	servers		map[string]chan struct{}
	closed		bool
	close 		chan struct{}
}

func (r *Replicator) Join(name, addr string) error {

	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])

	return nil
}

