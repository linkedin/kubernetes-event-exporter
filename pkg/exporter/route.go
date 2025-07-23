package exporter

import "github.com/resmoio/kubernetes-event-exporter/pkg/kube"

// Route allows using rules to drop events or match events to specific receivers.
// It also allows using routes recursively for complex route building to fit
// most of the needs
type Route struct {
	Drop   []Rule
	Match  []Rule
	Routes []Route
}

func (r *Route) Process(payload kube.Payload, registry ReceiverRegistry) {
	// First determine whether we will drop the event: If any of the drop is matched, we break the loop
	for _, v := range r.Drop {
		if v.Matches(payload) {
			return
		}
	}

	// It has match rules, it should go to the matchers
	matchesAll := true
	for _, rule := range r.Match {
		if rule.Matches(payload) {
			if rule.Receiver != "" {
				if event, ok := payload.(*kube.EnhancedEvent); ok {
					registry.SendEvent(rule.Receiver, event)
				} else if object, ok := payload.(*kube.Object); ok {
					registry.SendObject(rule.Receiver, object)
				}
			}
		} else {
			matchesAll = false
		}
	}

	// If all matches are satisfied, we can send them down to the rabbit hole
	if matchesAll {
		for _, subRoute := range r.Routes {
			subRoute.Process(payload, registry)
		}
	}
}
