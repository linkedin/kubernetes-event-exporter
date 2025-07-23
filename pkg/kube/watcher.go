package kube

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/resmoio/kubernetes-event-exporter/pkg/metrics"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
)

// TODO: use clock.PassiveClock instead of time.Now() for testing
var startUpTime = time.Now()

type EventHandler func(event *EnhancedEvent)

type EventWatcher struct {
	wg                  sync.WaitGroup
	informer            cache.SharedInformer
	stopper             chan struct{}
	objectMetadataCache ObjectMetadataProvider
	omitLookup          bool
	fn                  EventHandler
	maxEventAgeSeconds  time.Duration
	metricsStore        *metrics.Store
	dynamicClient       *dynamic.DynamicClient
	clientset           *kubernetes.Clientset
}

func NewEventWatcher(config *rest.Config, namespace string, MaxEventAgeSeconds int64, metricsStore *metrics.Store, fn EventHandler, omitLookup bool, cacheSize int) *EventWatcher {
	clientset := kubernetes.NewForConfigOrDie(config)
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(namespace))
	informer := factory.Core().V1().Events().Informer()

	watcher := &EventWatcher{
		informer:            informer,
		stopper:             make(chan struct{}),
		objectMetadataCache: NewObjectMetadataProvider(cacheSize),
		omitLookup:          omitLookup,
		fn:                  fn,
		maxEventAgeSeconds:  time.Second * time.Duration(MaxEventAgeSeconds),
		metricsStore:        metricsStore,
		dynamicClient:       dynamic.NewForConfigOrDie(config),
		clientset:           clientset,
	}

	informer.AddEventHandler(watcher)
	informer.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
		watcher.metricsStore.WatchErrors.Inc()
	})

	return watcher
}

func (e *EventWatcher) OnAdd(obj interface{}) {
	event := obj.(*corev1.Event)
	e.onEvent(event)
}

func (e *EventWatcher) OnUpdate(oldObj, newObj interface{}) {
	event := newObj.(*corev1.Event)
	e.onEvent(event)
}

// Ignore events older than the maxEventAgeSeconds.
func (e *EventWatcher) isEventDiscarded(event *corev1.Event) bool {
	eventAge, timeUsedForAge := getEventAge(event, clock.RealClock{})
	if eventAge > e.maxEventAgeSeconds {
		// Log discarded events if they were created after the watcher started
		// (to suppres warnings from initial synchronization)
		if timeUsedForAge.After(startUpTime) {
			log.Warn().
				Str("eventAge", eventAge.String()).
				Str("eventNamespace", event.Namespace).
				Str("eventName", event.Name).
				Str("eventMessage", event.Message).
				Str("eventReason", event.Reason).
				Str("eventSourceComponent", event.Source.Component).
				Str("eventSourceHost", event.Source.Host).
				Str("eventFirstTimestamp", event.FirstTimestamp.String()).
				Str("eventLastTimestamp", event.LastTimestamp.String()).
				Str("eventCreationTime", event.CreationTimestamp.String()).
				Str("eventEventTime", event.EventTime.String()).
				Str("involvedObjectAPIVersion", event.InvolvedObject.APIVersion).
				Str("involvedObjectKind", event.InvolvedObject.Kind).
				Str("involvedObjectNamespace", event.InvolvedObject.Namespace).
				Str("involvedObjectName", event.InvolvedObject.Name).
				Str("involvedObjectUID", string(event.InvolvedObject.UID)).
				Msg(fmt.Sprintf("Event discarded as being older than maxEventAgeSeconds: %v", e.maxEventAgeSeconds))
			e.metricsStore.EventsDiscarded.Inc()
		}
		return true
	}
	return false
}

// getEventAge returns the age of the event and the time used for age calculation.
// The time used for age calculation is the greater of creationTimestamp and
// lastTimestamp. While based on event aggregation, lastTimestamp should be greater
// than creationTimestamp, in practice it doesn't always seem to be the case.
// Hence, we compare and use the greater of the two to ensure that we process the
// event and don't skip it.
func getEventAge(event *corev1.Event, clock clock.PassiveClock) (time.Duration, time.Time) {
	var timeUsedForAgeCalculation time.Time

	if event.CreationTimestamp.Time.After(event.LastTimestamp.Time) {
		timeUsedForAgeCalculation = event.CreationTimestamp.Time
	} else {
		timeUsedForAgeCalculation = event.LastTimestamp.Time
	}

	return clock.Since(timeUsedForAgeCalculation), timeUsedForAgeCalculation
}

func (e *EventWatcher) onEvent(event *corev1.Event) {
	if e.isEventDiscarded(event) {
		return
	}

	log.Debug().
		Str("msg", event.Message).
		Str("namespace", event.Namespace).
		Str("reason", event.Reason).
		Str("involvedObject", event.InvolvedObject.Name).
		Msg("Received event")

	e.metricsStore.EventsProcessed.Inc()

	ev := &EnhancedEvent{
		Event: *event.DeepCopy(),
	}
	ev.Event.ManagedFields = nil

	if e.omitLookup {
		ev.InvolvedObject.ObjectReference = *event.InvolvedObject.DeepCopy()
	} else {
		objectMetadata, err := e.objectMetadataCache.GetObjectMetadata(&event.InvolvedObject, e.clientset, e.dynamicClient, e.metricsStore)
		if err != nil {
			if errors.IsNotFound(err) {
				ev.InvolvedObject.Deleted = true
				log.Error().Err(err).Msg("Object not found, likely deleted")
			} else {
				log.Error().Err(err).Msg("Failed to get object metadata")
			}
			ev.InvolvedObject.ObjectReference = *event.InvolvedObject.DeepCopy()
		} else {
			ev.InvolvedObject.Labels = objectMetadata.Labels
			ev.InvolvedObject.Annotations = objectMetadata.Annotations
			ev.InvolvedObject.OwnerReferences = objectMetadata.OwnerReferences
			ev.InvolvedObject.ObjectReference = *event.InvolvedObject.DeepCopy()
			ev.InvolvedObject.Deleted = objectMetadata.Deleted
		}
	}

	e.fn(ev)
}

func (e *EventWatcher) OnDelete(obj interface{}) {
	// Ignore deletes
}

func (e *EventWatcher) Start() {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.informer.Run(e.stopper)
	}()
}

func (e *EventWatcher) Stop() {
	close(e.stopper)
	e.wg.Wait()
}

func (e *EventWatcher) setStartUpTime(time time.Time) {
	startUpTime = time
}

type ObjectHandler func(object *Object)

// ObjectWatcher watches for Kubernetes objects and sends them to a channel.
// Currently, it is hardcoded to watch for Pods only.
type ObjectWatcher struct {
	wg        sync.WaitGroup
	informer  cache.SharedInformer
	stopper   chan struct{}
	fn        ObjectHandler
	clientset kubernetes.Interface
}

// NewObjectWatcher creates a new ObjectWatcher from a Kubernetes config.
func NewObjectWatcher(config *rest.Config, namespace string, fn ObjectHandler) *ObjectWatcher {
	clientset := kubernetes.NewForConfigOrDie(config)
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(namespace))
	informer := factory.Core().V1().Pods().Informer()

	watcher := &ObjectWatcher{
		informer:  informer,
		stopper:   make(chan struct{}),
		fn:        fn,
		clientset: clientset,
	}

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    watcher.onAdd,
		UpdateFunc: watcher.onUpdate,
		DeleteFunc: watcher.onDelete,
	})

	return watcher
}

func (o *ObjectWatcher) Start() {
	log.Info().Msg("Starting object watcher (for pods)")
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		o.informer.Run(o.stopper)
	}()
}

func (o *ObjectWatcher) Stop() {
	log.Info().Msg("Stopping object watcher")
	close(o.stopper)
	o.wg.Wait()
}

func (o *ObjectWatcher) onAdd(obj interface{}) {
	o.handleObject(obj, "added")
}

func (o *ObjectWatcher) onUpdate(oldObj, newObj interface{}) {
	o.handleObject(newObj, "updated")
}

func (o *ObjectWatcher) onDelete(obj interface{}) {
	// Ignore deletes
}

func (o *ObjectWatcher) handleObject(obj interface{}, eventType string) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		log.Error().Interface("object", obj).Msg("Failed to cast object to *corev1.Pod")
		return
	}

	// Convert the typed Pod object to our generic kube.Object for processing.
	unstructuredMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		log.Error().Err(err).Msg("Failed to convert pod to unstructured")
		return
	}

	kubeObj := &Object{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredMap, kubeObj); err != nil {
		log.Error().Err(err).Msg("Failed to convert unstructured object to structured object")
		return
	}

	kubeObj.Kind = "Pod"
	kubeObj.APIVersion = "v1"
	kubeObj.EventType = eventType

	// The unstructured conversion doesn't preserve all metadata fields, so we copy them manually.
	kubeObj.ObjectMeta = *pod.ObjectMeta.DeepCopy()

	// Extract spec and status as raw JSON.
	spec, _, _ := unstructured.NestedFieldCopy(unstructuredMap, "spec")
	status, _, _ := unstructured.NestedFieldCopy(unstructuredMap, "status")

	kubeObj.Spec, _ = json.Marshal(spec)
	kubeObj.Status, _ = json.Marshal(status)

	o.fn(kubeObj)
}
