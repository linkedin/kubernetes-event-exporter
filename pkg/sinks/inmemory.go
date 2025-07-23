package sinks

import (
	"context"
	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
)

type InMemoryConfig struct {
	Ref *InMemory
}

type InMemory struct {
	Events []*kube.EnhancedEvent
	Config *InMemoryConfig
}

func (s *InMemory) Send(ctx context.Context, payload kube.Payload) error {
	if ev, ok := payload.(*kube.EnhancedEvent); ok {
		s.Events = append(s.Events, ev)
	}
	return nil
}

func (i *InMemory) Close() {
	// No-op
}


