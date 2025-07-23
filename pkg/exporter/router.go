package exporter

import "github.com/resmoio/kubernetes-event-exporter/pkg/kube"

type Router struct {
	cfg  *Config
	rcvr ReceiverRegistry
}

func (r *Router) ProcessEvent(event *kube.EnhancedEvent) {
	r.cfg.Route.Process(event, r.rcvr)
}

func (r *Router) ProcessObject(object *kube.Object) {
	r.cfg.Route.Process(object, r.rcvr)
}
