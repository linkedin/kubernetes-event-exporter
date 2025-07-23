package exporter

import (
	"testing"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/resmoio/kubernetes-event-exporter/pkg/sinks"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// testReceiverRegistry just records the events to the registry so that tests can validate routing behavior
type testReceiverRegistry struct {
	ercvd map[string][]*kube.EnhancedEvent
	orcvd map[string][]*kube.Object
}

func (t *testReceiverRegistry) Register(string, sinks.Sink) {
	panic("Why do you call this? It's for counting imaginary events for tests only")
}

func (t *testReceiverRegistry) SendEvent(name string, event *kube.EnhancedEvent) {
	if t.ercvd == nil {
		t.ercvd = make(map[string][]*kube.EnhancedEvent)
	}

	if _, ok := t.ercvd[name]; !ok {
		t.ercvd[name] = make([]*kube.EnhancedEvent, 0)
	}

	t.ercvd[name] = append(t.ercvd[name], event)
}

func (t *testReceiverRegistry) SendObject(name string, object *kube.Object) {
	if t.orcvd == nil {
		t.orcvd = make(map[string][]*kube.Object)
	}

	if _, ok := t.orcvd[name]; !ok {
		t.orcvd[name] = make([]*kube.Object, 0)
	}

	t.orcvd[name] = append(t.orcvd[name], object)
}

func (t *testReceiverRegistry) Close() {
	// No-op
}

func (t *testReceiverRegistry) isEventRcvd(name string, event *kube.EnhancedEvent) bool {
	if val, ok := t.ercvd[name]; !ok {
		return false
	} else {
		for _, v := range val {
			if v == event {
				return true
			}
		}
		return false
	}
}

func (t *testReceiverRegistry) isObjectRcvd(name string, object *kube.Object) bool {
	if val, ok := t.orcvd[name]; !ok {
		return false
	} else {
		for _, v := range val {
			if v == object {
				return true
			}
		}
		return false
	}
}

func (t *testReceiverRegistry) count(name string) int {
	if val, ok := t.ercvd[name]; ok {
		return len(val)
	} else {
		return 0
	}
}

func TestEmptyRoute(t *testing.T) {
	ev := kube.EnhancedEvent{}
	reg := testReceiverRegistry{}

	r := Route{}

	r.Process(&ev, &reg)
	assert.Empty(t, reg.ercvd)
}

func TestBasicRoute(t *testing.T) {
	ev := kube.EnhancedEvent{}
	ev.Namespace = "kube-system"
	reg := testReceiverRegistry{}

	r := Route{
		Match: []Rule{{
			Namespace: "kube-system",
			Receiver:  "osman",
		}},
	}

	r.Process(&ev, &reg)
	assert.True(t, reg.isEventRcvd("osman", &ev))
}

func TestDropRule(t *testing.T) {
	ev := kube.EnhancedEvent{}
	ev.Namespace = "kube-system"
	reg := testReceiverRegistry{}

	r := Route{
		Drop: []Rule{{
			Namespace: "kube-system",
		}},
		Match: []Rule{{
			Receiver: "osman",
		}},
	}

	r.Process(&ev, &reg)
	assert.False(t, reg.isEventRcvd("osman", &ev))
	assert.Zero(t, reg.count("osman"))
}

func TestSingleLevelMultipleMatchRoute(t *testing.T) {
	ev := kube.EnhancedEvent{}
	ev.Namespace = "kube-system"
	reg := testReceiverRegistry{}

	r := Route{
		Match: []Rule{{
			Namespace: "kube-system",
			Receiver:  "osman",
		}, {
			Receiver: "any",
		}},
	}

	r.Process(&ev, &reg)
	assert.True(t, reg.isEventRcvd("osman", &ev))
	assert.True(t, reg.isEventRcvd("any", &ev))
}

func TestSubRoute(t *testing.T) {
	ev := kube.EnhancedEvent{}
	ev.Namespace = "kube-system"
	reg := testReceiverRegistry{}

	r := Route{
		Match: []Rule{{
			Namespace: "kube-system",
			Receiver:  "osman",
		}},
		Routes: []Route{{
			Match: []Rule{{
				Receiver: "osman",
			}},
		}},
	}

	r.Process(&ev, &reg)

	assert.True(t, reg.isEventRcvd("osman", &ev))
}

func TestSubSubRoute(t *testing.T) {
	ev := kube.EnhancedEvent{}
	ev.Namespace = "kube-system"
	reg := testReceiverRegistry{}

	r := Route{
		Match: []Rule{{
			Namespace: "kube-*",
		}},
		Routes: []Route{{
			Match: []Rule{{
				Receiver: "osman",
			}},
			Routes: []Route{{
				Match: []Rule{{
					Receiver: "any",
				}},
			}},
		}},
	}

	r.Process(&ev, &reg)

	assert.True(t, reg.isEventRcvd("osman", &ev))
	assert.True(t, reg.isEventRcvd("any", &ev))
}

func TestSubSubRouteWithDrop(t *testing.T) {
	ev := kube.EnhancedEvent{}
	ev.Namespace = "kube-system"
	reg := testReceiverRegistry{}

	r := Route{
		Match: []Rule{{
			Namespace: "kube-*",
		}},
		Routes: []Route{{
			Match: []Rule{{
				Receiver: "osman",
			}},
			Routes: []Route{{
				Drop: []Rule{{
					Namespace: "kube-system",
				}},
				Match: []Rule{{
					Receiver: "any",
				}},
			}},
		}},
	}

	r.Process(&ev, &reg)

	assert.True(t, reg.isEventRcvd("osman", &ev))
	assert.False(t, reg.isEventRcvd("any", &ev))
}

// Test for issue: https://github.com/resmoio/kubernetes-event-exporter/issues/51
func Test_GHIssue51(t *testing.T) {
	ev1 := kube.EnhancedEvent{}
	ev1.Type = "Warning"
	ev1.Reason = "FailedCreatePodContainer"

	ev2 := kube.EnhancedEvent{}
	ev2.Type = "Warning"
	ev2.Reason = "FailedCreate"

	reg := testReceiverRegistry{}

	r := Route{
		Drop: []Rule{{
			Type: "Normal",
		}},
		Match: []Rule{{
			Reason:   "FailedCreatePodContainer",
			Receiver: "elastic",
		}},
	}

	r.Process(&ev1, &reg)
	r.Process(&ev2, &reg)

	assert.True(t, reg.isEventRcvd("elastic", &ev1))
	assert.False(t, reg.isEventRcvd("elastic", &ev2))
}

func TestBasicObjectRoute(t *testing.T) {
	obj := &kube.Object{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
		},
	}
	reg := testReceiverRegistry{}

	r := Route{
		Match: []Rule{{
			Namespace: "kube-system",
			Receiver:  "osman",
		}},
	}

	r.Process(obj, &reg)
	assert.True(t, reg.isObjectRcvd("osman", obj))
}
